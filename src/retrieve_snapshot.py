import requests
import os
import logging
from datetime import datetime
import csv
import warcio
from constants import TARGET_DATE
import cdx_toolkit

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_warc_url(cdx_url):
    """Query the CDX API to get the closest WARC file URL before or on the target date."""
    # Format the CDX API URL for the query
    cdx_api_url = f"https://web.archive.org/cdx/search/cdx?url={cdx_url}&output=json&fl=timestamp,original,warc_url&limit=25000"

    # Send the request to the CDX API
    response = requests.get(cdx_api_url)

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
    response = requests.get(cdx_api_url)

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


def download_warc(warc_url, save_path):
    """Download a WARC file from the Wayback Machine."""
    # Send a request to download the WARC file
    response = requests.get(warc_url, stream=True)

    # Check if the response is valid
    if response.status_code == 200:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # Save the WARC file
        with open(save_path, "wb") as warc_file:
            for chunk in response.iter_content(chunk_size=8192):
                warc_file.write(chunk)
        logging.info(f"WARC saved to {save_path}")
    else:
        logging.error(
            f"Failed to download WARC file from {warc_url} (status code: {response.status_code})"
        )


def extract_warc_components(warc_path, save_dir):
    """Extract HTML, CSS, and images from a WARC file and save them in the correct directory."""
    with open(warc_path, "rb") as warc_file:
        archive = warcio.archiveiterator.ArchiveIterator(warc_file)

        for record in archive:
            url = record.rec_headers.get_header("WARC-Target-URI")
            content_type = record.http_headers.get_header("Content-Type")
            payload = record.content

            # Save HTML files
            if "html" in content_type:
                relative_path = os.path.join(
                    save_dir, url.lstrip("https://www.cdc.gov")
                )
                os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                with open(relative_path, "wb") as html_file:
                    html_file.write(payload)
                logging.info(f"Saved HTML file: {relative_path}")

            # Save CSS files
            elif "css" in content_type:
                relative_path = os.path.join(
                    save_dir, url.lstrip("https://www.cdc.gov")
                )
                os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                with open(relative_path, "wb") as css_file:
                    css_file.write(payload)
                logging.info(f"Saved CSS file: {relative_path}")

            # Save image files
            elif "image" in content_type:
                relative_path = os.path.join(
                    save_dir, url.lstrip("https://www.cdc.gov")
                )
                os.makedirs(os.path.dirname(relative_path), exist_ok=True)
                with open(relative_path, "wb") as img_file:
                    img_file.write(payload)
                logging.info(f"Saved image file: {relative_path}")


def download_warc_cdx_toolkit(url, best_timestamp, warc_save_path):
    """
    Adapted from example: https://github.com/cocrawler/cdx_toolkit/blob/main/examples/iter-and-warc.py
    :param url:
    :return:
    """
    cdx = cdx_toolkit.CDXFetcher(source="ia")
    warcinfo = {
        "software": "pypi_cdx_toolkit iter-and-warc example",
        "isPartOf": "CDC",  # 'EXAMPLE-COMMONCRAWL',
        "description": "warc extraction",
        "format": "WARC file version 1.0",
    }

    # TODO verify the conventions of warc info, and the conventions of using subprefix (set to "test" right now).
    writer = cdx_toolkit.warc.get_writer(
        warc_save_path, "test", warcinfo, warc_version="1.1"
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
                logging.info("Skipping because status was {}, not 200".format(status))
                continue
            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                logging.info(
                    "Skipping capture for RuntimeError 404: %s %s", url, timestamp
                )
                continue
            writer.write_record(record)
            logging.info(f"Wrote warc for {url}")


def process_cdc_urls(csv_filename, base_dir):
    """Process a list of URLs, download the closest WARC snapshot, and extract resources."""
    with open(csv_filename, "r") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            url = row[0].strip()

            # Define the path where to save the WARC file
            warc_filename = (
                url.replace("https://www.cdc.gov", "").replace("/", "_") + ".warc.gz"
            )
            warc_save_path = os.path.join(base_dir, "warcs", warc_filename)

            # Download the WARC file for the best timestamp
            timestamp = get_best_date_for_url(url)
            download_warc_cdx_toolkit(url, timestamp, warc_save_path)

            # Extract components from the WARC file
            # extract_warc_components(warc_save_path, base_dir)
