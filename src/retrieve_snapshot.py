# Standard library imports
import csv
import json
import logging
import os
import time
from datetime import datetime

# Third-party imports
import requests
import cdx_toolkit

# Local application/library imports
from constants import TARGET_DATE

def get_with_retries(url, max_retries=5, delay=3, failed_urls=None, track_failures=True):
    """
    Handling requests with retries and timeouts
    :param url: URL to try
    :param max_retries: integer for maximum number of retries to attempt
    :param delay: time in seconds for delay calculations
    :param failed_urls: optional list to collect URLs that fail all attempts
    :param track_failures: if True, append failed URL to list
    :return: Response object or None
    """
    for attempt in range(max_retries):
        try:
            return requests.get(url, timeout=30)
        except requests.exceptions.ReadTimeout as e:
            logging.error(f"Attempt {attempt+1} timed out for URL {url}: {e}")
            time.sleep(delay * (2 ** attempt))
            continue
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Attempt {attempt+1} connection failed for URL {url}: {e}")
            time.sleep(delay * (2 ** attempt))
            continue
        except Exception as e:
            logging.error(f"Unexpected error for URL {url} on attempt {attempt+1}: {e}")
            continue
    logging.warning(f"Skipping URL after {max_retries} failed attempts: {url}")
    if failed_urls is not None and track_failures:
        failed_urls.append(url)
    return None


def get_warc_url(cdx_url):
    """
    Query the CDX API to get the closest WARC file URL before or on the target date.
    :param cdx_url: URL used to find archive for
    :return none
    """
    # Format the CDX API URL for the query
    cdx_api_url = (
        f"https://web.archive.org/cdx/search/cdx?"
        f"url={cdx_url}"
        f"&output=json"
        f"&fl=timestamp,original,warc_url"
        f"&limit=-1"
        f"&to=20250119"
    )
    # Send the request to the CDX API
    response = get_with_retries(cdx_api_url)

    if not response:
        logging.error(
            f"Failed to reach CDX API entirely."
        )
        return None

    if response.status_code != 200:
        logging.error(
            f"Failed to query CDX API. HTTP status code: {response.status_code}"
        )
        return None

    data = response.json()

    # Filter snapshots that are on or before the target date
    closest_snapshot = None
    for entry in data[1:]:
        timestamp = entry[0]
        snapshot_date = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        if snapshot_date <= TARGET_DATE:
            closest_snapshot = entry
        else:
            break  # Since the data is sorted by date, no need to check further snapshots

    if not closest_snapshot:
        logging.error("No snapshots found before the target date.")
        return None
    # Extract the WARC URL and timestamp
    warc_url = closest_snapshot[1]
    timestamp = closest_snapshot[0]
    logging.info(
        f"Found closest snapshot: {warc_url} for timestamp {timestamp}"
    )
    return warc_url


def get_best_date_for_url(cdx_url):
    """
    Query the CDX API to get the closest WARC file URL before or on the target date.
    :param cdx_url: URL to find archive for
    :return none
    """
    # Format the CDX API URL for the query
    # TODO A limit of 25,000 could make a tough bug to find; 
    # if we run this in the future, and web.archive.org has made
    # more than that many backups, we could end up not seeing the right version.
    cdx_api_url = (
        f"https://web.archive.org/cdx/search/cdx?"
        f"url={cdx_url}"
        f"&output=json"
        f"&fl=timestamp,original,warc_url"
        f"&limit=-1"
        f"&to=20250119"
    )
    
    # Send the request to the CDX API
    response = get_with_retries(cdx_api_url)

    if not response:
        logging.error(
            f"Failed to reach CDX API entirely."
        )
        return None

    if response.status_code != 200:
        logging.error(
            f"Failed to query CDX API. HTTP status code: {response.status_code}"
        )
        return None

    data = response.json()

    # Filter snapshots that are on or before the target date
    closest_snapshot = None
    for entry in data[1:]:
        timestamp = entry[0]
        snapshot_date = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        if snapshot_date <= TARGET_DATE:
            closest_snapshot = entry
        else:
            break  # Since the data is sorted by date, no need to check further snapshots

    if not closest_snapshot:
        logging.error("No snapshots found before the target date.")
        return None

    # Extract the WARC URL and timestamp
    url = closest_snapshot[1]
    timestamp = closest_snapshot[0]
    logging.info(f"Found closest snapshot: {url} for timestamp {timestamp}")
    return timestamp


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

    issues_seen = False
    match_found = False
    #TODO Possible loss of data if it goes beyond this limit; ideally there's a method to check the size of cdx but there isn't always
    for obj in cdx.iter(url, limit=10):
        timestamp = obj["timestamp"]
        if timestamp == best_timestamp:
            match_found = True
            url = obj["url"]
            status = obj["status"]
            logging.info(
                f"Considering extracting url: {url} with timestamp {timestamp}"
            )
            if status != "200":
                logging.debug("Skipping because status was {}, not 200".format(status))
                issues_seen = True
                continue
            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                logging.debug(
                    "Skipping capture for RuntimeError 404: %s %s", url, timestamp
                )
                issues_seen = True
                continue
            writer.write_record(record)
            files.append(writer.filename)
            logging.info(f"********SUCCESS!********** Wrote warc for {url}")
    if not match_found:
        issues_seen = True
    return files, issues_seen

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
        for path in paths:
            url = os.path.join(subdomain + path)
            if path in fetched_state:
                logging.info(f"Previous result for {path}: {fetched_state[path]}")
                if fetched_state[path]['issues']:
                    failed_urls.append(url)
                continue
            logging.info(f"========== Processing URL: {url} ==========")

            # Define the prefix for saving the WARC segments
            warc_filename = (
                url.replace("/", "_") 
            )
            
            warc_save_path = os.path.join(base_dir, warc_filename)
            
            #logging.debug(f"$warc_save_path: {warc_save_path}")
            #logging.debug(f"$warc_filename: {warc_filename}")

            #check if warc already exists
            if os.path.exists(warc_save_path):
                logging.debug(f"warc exists: {warc_save_path}")
                continue
                
            if url in failed_urls:
                logging.warning(f"Skipping processing for failed URL: {url}")
                continue
                
            timestamp = get_best_date_for_url(url)
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
