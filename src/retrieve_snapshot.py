# Standard library imports
import copy
import json
import logging
import os

import urllib.parse

# ======================================================================
# Monkey patching the urllib.parse.quote() function (as an alternative
# to completely rewriting the fetch_warc_record() function, which I'd
# like to avoid).

orig_quote = urllib.parse.quote

def customized_quote(url):
    """
    If there is a "?" in the URL, only quote what follows after it.

    :param url: a URL path
    :return: the URL path, appropriately quoted for use with warc.py functions
    """
    offset1 = url.find("?")
    if offset1 == -1:
        return url
    pre_q  = url[:offset1+1]
    post_q = url[offset1+1:]

    # Experiment:
    return pre_q + orig_quote(post_q)

urllib.parse.quote = customized_quote

# Done with the monkey patching.
# ======================================================================

# Third-party imports
import cdx_toolkit

# The documented rate limit is 15 requests per minute, so in theory
# this should keep us on track for that.
#
# https://archive.org/details/toomanyrequests_20191110
cdx_toolkit.myrequests.retry_info['web.archive.org']['minimum_interval'] = 4.0

# The one thing slowing this down right now is the retry_info settings
# in cdx_toolkit/myrequests.py which prescribe a minimum interval of 6
# seconds between requests. A sensible interval, but it means several
# subdomains are going to take quite some time to collect.
def download_warc_cdx_toolkit(subdomain, url_data, warc_save_path):
    """
    Adapted from example: https://github.com/cocrawler/cdx_toolkit/blob/main/examples/iter-and-warc.py
    :param url: a URL to find WARC
    :param best_timestamp: a datetime used to find WARC
    :param warc_save_path: a path where the WARC will be saved
    :return: the .warc(.gz) file name, and a flag indicating if we noticed any problems
    """
    url = url_data['original']
    timestamp = url_data['timestamp']
    warc_file = None
    logging.debug(f"Attempting to download warc for {url}")
    cdx = cdx_toolkit.CDXFetcher(source="ia")
    warcinfo = {
        "software": "pypi_cdx_toolkit iter-and-warc example",
        "isPartOf": "CDC", 
        "description": "warc extraction",
        "format": "WARC file version 1.0",
    }

    #logging.debug(f"$warc_save_path: {warc_save_path}")

    writer = cdx_toolkit.warc.get_writer(
        warc_save_path, "", warcinfo
    )

    # Copying the 'wb' value that cdx_toolkit.CDXFetcher.__init__()
    # sets for source="ia":
    data = copy.deepcopy(url_data)
    data['url'] = f"https://{subdomain}{data['path']}"
    # del data['path']
    # del data['original']
    data['status'] = data['statuscode']
    # del data['statuscode']
    data['mime'] = data['mimetype']
    # del data['statuscode']
    obj = cdx_toolkit.CaptureObject(data, wb='https://web.archive.org/web')

    try:
        record = obj.fetch_warc_record()
    except RuntimeError:
        logging.debug("Skipping capture for RuntimeError 404: %s %s", url, timestamp)
        return None, True

    writer.write_record(record)
    warc_file = writer.filename
    logging.info(f"********SUCCESS!********** Wrote warc for {url} at {warc_file}")
    return warc_file, False

def process_cdc_urls(state_folder, base_dir, track_failed_urls, retry_failed_urls, failed_urls, subdomains, ldb):
    """
    Process a list of URLs, download the closest WARC snapshot, and extract resources.
    :param state_folder: Folder in which to track/cache the URLs we've already processed before
    :param base_dir: a file location to save the WARC
    :param track_failed_urls: flag to indicate whether to track failed URLs
    :param retry_failed_urls: flag to indicate whether to retry previously failed URLs
    :param failed_urls: a file where failed URLs are logged
    :param subdomains: a nested list of subdomains and paths to use to find WARC archives
    :param ldb: a WARCLevelDB instance; to give process_url() calls as we go
    :return: a list of failed URLs, plus an extended version of the subdomains structure
    """

    failed_urls = []

    url_list_plus = {}

    for subdomain, paths in subdomains.items():
        url_list_plus[subdomain] = []
        fetched_file = f"{state_folder}/fetched.{subdomain}.json"
        if os.path.exists(fetched_file):
            with open(fetched_file, "r", encoding="utf-8") as fetched_fd:
                fetched_state = json.load(fetched_fd)
                fetched_fd.close()
        else:
            fetched_state = {}
        total = len(paths)
        for ix, url_data in enumerate(paths):
            path = url_data['path']
            timestamp = url_data['timestamp']
            url = os.path.join(subdomain + path)  
            if path in fetched_state:
                logging.info(f"[{ix+1}/{total}] Previous result for {path}: {fetched_state[path]}")
                if fetched_state[path]['issues'] and retry_failed_urls:
                    logging.info("Retrying this URL...")
                else:
                    if fetched_state[path]['issues'] and track_failed_urls:
                        failed_urls.append(url)
                    url_data = copy.deepcopy(url_data)
                    url_data['fetched'] = fetched_state[path]
                    url_list_plus[subdomain].append(url_data)
                    if ldb and not fetched_state[path]['issues'] and fetched_state[path]['file']:
                        ldb.process_url(subdomain, url_data)
                    continue

            logging.info(f"[{ix+1}/{total}] ========== Processing URL: {url} [{timestamp}] ==========")

            # Define the prefix for saving the WARC segments
            warc_filename = (
                url.replace("/", "_") 
            )

            warc_save_path = os.path.join(base_dir, warc_filename)[:150]# need to truncate to avoid 
                                                        # filename length errors
                                                        # doesnt affect actual URL

            warc_file, issues = download_warc_cdx_toolkit(subdomain, url_data, warc_save_path)
            fetched_state[path] = { "file": warc_file, "issues": issues }
            if issues and track_failed_urls:
                failed_urls.append(url)

            url_data = copy.deepcopy(url_data)
            url_data['fetched'] = fetched_state[path]
            if ldb and not fetched_state[path]['issues'] and fetched_state[path]['file']:
                ldb.process_url(subdomain, url_data)

            # Write intermediate state to disk, so we can pick it up
            # if we abort the process or it crashes
            with open(fetched_file, "w", encoding="utf-8") as fetched_fd:
                json.dump(fetched_state, fetched_fd)
                fetched_fd.close()

    return failed_urls, url_list_plus
