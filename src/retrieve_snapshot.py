import requests
import os
import csv
import warcio
import logging
import cdx_toolkit
import time
from datetime import datetime
from constants import TARGET_DATE

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
)

def get_with_retries(url, max_retries=5, delay=3):
    """
    Handling requests with retries and timeouts
    :param url: URL to try
    :param max_retries: integer for maximum number of retries to attempt
    :param delay: time in seconds for delay calculations
    :return none
    """
    for attempt in range(max_retries):
        try:
            return requests.get(url, timeout=30)
        except requests.exceptions.ReadTimeout as e:
            print(f"Attempt {attempt+1} timed out: {e}")
            logging.debug("URL retry delay")
            time.sleep(delay* (2 ** attempt))
            continue
        except requests.exceptions.ConnectionError as e:
            logging.debug(f"Attempt {attempt + 1} failed: {e}")
            logging.debug("URL retry delay")
            time.sleep(delay* (2 ** attempt))
            continue
        except Exception as e:
            logging.warning(f"Skipping URL due to repeated failure: {url} — {str(e)}")
            continue
    logging.warning(f"Skipping URL after {max_retries} failed attempts: {url}")
    return None  # <-- Don't raise, just return None

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
        f"&limit=25000"
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
        f"&limit=25000"
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
    :return none
    """
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

    #TODO Possible loss of data if it goes beyond this limit; ideally there's a method to check the size of cdx but there isn't always
    for obj in cdx.iter(url, limit=10):
        timestamp = obj["timestamp"]
        if timestamp == best_timestamp:
            url = obj["url"]
            status = obj["status"]
            logging.info(
                f"Considering extracting url: {url} with timestamp {timestamp}"
            )
            if status != "200":
                logging.debug("Skipping because status was {}, not 200".format(status))
                continue
            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                logging.debug(
                    "Skipping capture for RuntimeError 404: %s %s", url, timestamp
                )
                continue
            writer.write_record(record)
            logging.info(f"********SUCCESS!********** Wrote warc for {url}")


def process_cdc_urls(subdomains, base_dir):
    """
    Process a list of URLs, download the closest WARC snapshot, and extract resources.
    :param subdomains: a nested list of subdomains and paths to use to find WARC archives
    :param base_dir: a file location to save the WARC
    :return none
    """

    for subdomain, paths in subdomains.items():
        for path in paths:
            url = os.path.join(subdomain + path)
            logging.debug(f"==================== NEW URL ====================")
            logging.debug(f"$url {url}")

        # Define the path where to save the WARC file
            warc_filename = (
                url.replace("/", "_") 
            )
            
            warc_save_path = os.path.join(base_dir, warc_filename)
            
            #logging.debug(f"$warc_save_path: {warc_save_path}")
            #logging.debug(f"$warc_filename: {warc_filename}")

            #check if warc already exists
            if os.path.exists(warc_save_path):
                logging.debug(f"warc exists: {warc_save_path}")
                pass
            else:
                # Download the WARC file for the best timestamp
                timestamp = get_best_date_for_url(url)
                download_warc_cdx_toolkit(url, timestamp, warc_save_path)
