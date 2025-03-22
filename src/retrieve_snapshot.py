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

def get_with_retries(url, max_retries=3, delay=3):
    for attempt in range(max_retries):
        try:
            return requests.get(url, timeout=30)
        except requests.exceptions.ReadTimeout as e:
            print(f"Attempt {attempt+1} timed out: {e}")
            logging.debug("URL retry delay")
            time.sleep(delay* (2 ** attempt))
        except requests.exceptions.ConnectionError as e:
            logging.debug(f"Attempt {attempt + 1} failed: {e}")
            logging.debug("URL retry delay")
            time.sleep(delay* (2 ** attempt))
        except Exception as e:
            logging.warning(f"Skipping URL due to repeated failure: {url} — {str(e)}")
            continue
        raise Exception(f"Failed to connect to {url} after {max_retries} attempts.")

def get_warc_url(cdx_url):
    """Query the CDX API to get the closest WARC file URL before or on the target date."""
    # Format the CDX API URL for the query
    cdx_api_url = f"https://web.archive.org/cdx/search/cdx?url={cdx_url}&output=json&fl=timestamp,original,warc_url&limit=25000"

    # Send the request to the CDX API
    response = get_with_retries(cdx_api_url)

    if response.status_code == 200:
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

        if closest_snapshot:
            # Extract the WARC URL and timestamp
            warc_url = closest_snapshot[1]
            timestamp = closest_snapshot[0]
            logging.info(
                f"Found closest snapshot: {warc_url} for timestamp {timestamp}"
            )
            return warc_url
        else:
            logging.error("No snapshots found before the target date.")
            return None
    else:
        logging.error(
            f"Failed to query CDX API. HTTP status code: {response.status_code}"
        )
        return None


def get_best_date_for_url(cdx_url):
    """Query the CDX API to get the closest WARC file URL before or on the target date."""
    # Format the CDX API URL for the query
    # TODO A limit of 25,000 could make a tough bug to find; if we run this in the future, and web.archive.org has made
    #  more than that many backups, we could end up not seeing the right version.
    cdx_api_url = f"https://web.archive.org/cdx/search/cdx?url={cdx_url}&output=json&fl=timestamp,original,warc_url&limit=25000"

    # Send the request to the CDX API
    response = get_with_retries(cdx_api_url)

    if response.status_code == 200:
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

        if closest_snapshot:
            # Extract the WARC URL and timestamp
            url = closest_snapshot[1]
            timestamp = closest_snapshot[0]
            logging.info(f"Found closest snapshot: {url} for timestamp {timestamp}")
            return timestamp
        else:
            logging.error("No snapshots found before the target date.")
            return None
    else:
        logging.error(
            f"Failed to query CDX API. HTTP status code: {response.status_code}"
        )
        return None


def download_warc_cdx_toolkit(url, best_timestamp, warc_save_path):
    """
    Adapted from example: https://github.com/cocrawler/cdx_toolkit/blob/main/examples/iter-and-warc.py
    :param url:
    :param best_timestamp
    :param warc_save_path
    :return: none
    """
    logging.debug(f"Attempting to download warc for {url}")
    cdx = cdx_toolkit.CDXFetcher(source="ia")
    warcinfo = {
        "software": "pypi_cdx_toolkit iter-and-warc example",
        "isPartOf": "CDC", 
        "description": "warc extraction",
        "format": "WARC file version 1.0",
    }

    # TODO verify the conventions of warc info, and the conventions of using subprefix (set to "test" right now).
    logging.debug(f"$warc_save_path: {warc_save_path}")
    
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
    """Process a list of URLs, download the closest WARC snapshot, and extract resources."""

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
            
            logging.debug(f"$warc_save_path: {warc_save_path}")
            logging.debug(f"$warc_filename: {warc_filename}")

            # Download the WARC file for the best timestamp
            timestamp = get_best_date_for_url(url)
            download_warc_cdx_toolkit(url, timestamp, warc_save_path)

        # Extract components from the WARC file
        # extract_warc_components(warc_save_path, base_dir)
