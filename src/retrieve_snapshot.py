# Standard library imports
import json
import logging
import os

# Third-party imports
import cdx_toolkit

# The one thing slowing this down right now is the retry_info settings
# in cdx_toolkit/myrequests.py which prescribe a minimum interval of 6
# seconds between requests. A sensible interval, but it means several
# subdomains are going to take quite some time to collect.
def download_warc_cdx_toolkit(url, best_timestamp, warc_save_path):
    """
    Adapted from example: https://github.com/cocrawler/cdx_toolkit/blob/main/examples/iter-and-warc.py
    :param url: a URL to find WARC
    :param best_timestamp: a datetime used to find WARC
    :param warc_save_path: a path where the WARC will be saved
    :return: A list of *.warc(.gz) files, and a flag indicating if we noticed any problems
    """
    files = []
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
    obj = cdx_toolkit.CaptureObject({
        'timestamp': best_timestamp,
        'url':       url,
        'status':    '200'
    }, wb='https://web.archive.org/web')
    
    try:
        record = obj.fetch_warc_record()
    except RuntimeError:
        logging.debug(
            "Skipping capture for RuntimeError 404: %s %s", url, best_timestamp
        )
        return [], True

    writer.write_record(record)
    files.append(writer.filename)
    logging.info(f"********SUCCESS!********** Wrote warc for {url} at {writer.filename}")
    return files, False

def process_cdc_urls(state_folder, base_dir, track_failed_urls, failed_urls, subdomains):
    """
    Process a list of URLs, download the closest WARC snapshot, and extract resources.
    :param state_folder: Folder in which to track/cache the URLs we've already processed before
    :param base_dir: a file location to save the WARC
    :param track_failed_urls: flag to indicate how to handle failed URLs
    :param failed_urls: a file where failed URLs are logged
    :param subdomains: a nested list of subdomains and paths to use to find WARC archives
    :return: a list of failed URLs
    """

    failed_urls = []

    for subdomain, paths in subdomains.items():
        fetched_file = f"{state_folder}/fetched.{subdomain}.json"
        if os.path.exists(fetched_file):
            with open(fetched_file, "r", encoding="utf-8") as fetched_fd:
                fetched_state = json.load(fetched_fd)
                fetched_fd.close()
        else:
            fetched_state = {}
        for url_data in paths:
            (path, timestamp) = url_data
            url = os.path.join(subdomain + path)  
            if path in fetched_state:
                logging.info(f"Previous result for {path}: {fetched_state[path]}")
                if fetched_state[path]['issues']:
                    failed_urls.append(url)
                continue
            logging.info(f"========== Processing URL: {url} [{timestamp}] ==========")

            # Define the prefix for saving the WARC segments
            warc_filename = (
                url.replace("/", "_") 
            )

            warc_save_path = os.path.join(base_dir, warc_filename)[:150]# need to truncate to avoid 
                                                        # filename length errors
                                                        # doesnt affect actual URL

            #logging.debug(f"$warc_save_path: {warc_save_path}")
            #logging.debug(f"$warc_filename: {warc_filename}")

            #check if warc already exists
            if os.path.exists(warc_save_path):
                logging.debug(f"warc exists: {warc_save_path}")
                continue

            if url in failed_urls:
                logging.warning(f"Skipping processing for failed URL: {url}")
                continue

            files, issues = download_warc_cdx_toolkit(url, timestamp, warc_save_path)
            fetched_state[path] = { "files": files, "issues": issues }
            if issues:
                failed_urls.append(url)

            # Write intermediate state to disk, so we can pick it up
            # if we abort the process or it crashes
            with open(fetched_file, "w", encoding="utf-8") as fetched_fd:
                json.dump(fetched_state, fetched_fd)
                fetched_fd.close()
    return failed_urls
