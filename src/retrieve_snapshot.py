
import cdx_toolkit
import csv
import logging
import warcat.model
import gzip
import os
import re
import requests
import shutil
import subprocess
from datetime import datetime
from constants import TARGET_DATE

def get_warc_url(cdx_url):
    """Query the CDX API to get the closest WARC file URL before or on the target date."""
    # Format the CDX API URL for the query
    cdx_api_url = f"https://web.archive.org/cdx/search/cdx?url={
                    cdx_url}&output=json&fl=timestamp,original,warc_url&limit=25000"

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
                break  

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
    # TODO A limit of 25,000 could make a tough bug to find; 
    # if we run this in the future, and web.archive.org has made
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
                break  

        if closest_snapshot:
            # Extract the WARC URL and timestamp
            url = closest_snapshot[1]
            timestamp = closest_snapshot[0]
            logging.info(
                f"Found closest snapshot: {url
                } for timestamp {timestamp}"
            )
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
        logging.info(f"save_path {save_path}")
    else:
        logging.error(
            f"Failed to download WARC file from warc_url {warc_url
            } (status code: {response.status_code})"
        )


def extract_warc_components(warc_path, warc_extracted):
    """Extract HTML, CSS, and images from a WARC file and save them in the 
    correct directory."""
    logging.info(f"Opening $warc_path: {warc_path}")
    '''
    #new_name = warc_path.replace("warc.gz", "warc")
    #logging.info(f"New name of file {new_name}")
    #TODO Error handling - if snapshot unavailable, expand time period to present
    # Open the .gz file and save the decompressed content
    if os.path.isfile(warc_path):
        with gzip.open(warc_path, "rb") as f_in:
            with open(new_name, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    '''
    command = ['python','-m','warcat', 'extract', 
                warc_path,'--output-dir', warc_extracted]
    result = subprocess.run(command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"All records extracted to {warc_extracted}")
    else:
        print(f"Error extracting records: {result.stderr}")
        

def download_warc_cdx_toolkit(url, best_timestamp, warc_path):
    """
    Adapted from example: https://github.com/cocrawler/cdx_toolkit/blob/main/
                            examples/iter-and-warc.py
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
    
    logging.info(f"$warc_path: {warc_path}")

    """
    TODO verify the conventions of warc info, and the conventions of 
    using subprefix (set to "test" right now). VERIFIED.
    """
    writer = cdx_toolkit.warc.get_writer(
        warc_path, "", warcinfo
    )
    
    """
    TODO Possible loss of data if it goes beyond this limit; 
    ideally there's a method to check the size of cdx but there isn't always
    """
    for obj in cdx.iter(url, limit=10):
        timestamp = obj["timestamp"]
        if timestamp == best_timestamp:
            url = obj["url"]
            status = obj["status"]
            logging.info(
                "Considering extracting WARC url%s with timestamp %s to %s", 
                url, timestamp, warc_path
                )
            if status != "200":
                logging.info(
                    "Skipping because status was {}, not 200".format(status)
                )
                continue
            try:
                record = obj.fetch_warc_record()
            except RuntimeError:
                logging.info(
                    "Skipping capture for RuntimeError 404: %s %s", 
                    url, timestamp
                )
                continue
            writer.write_record(record)
            logging.info(f"Wrote WARC for {url} at {warc_path}")

def get_highest_segment_file(fname, directory='.'):
    """
    Function that uses regular expressions to find the file with
    the highest digit and returns that filename
    """
    pattern = re.escape(rf'{fname}--') + r'(\d+)\.extracted\.warc\.gz$'
    highest_segment = -1
    highest_file = None

    for filename in os.listdir(directory):
        match = re.match(pattern, filename)  
        if match:
            segment = int(match.group(1))
            if segment > highest_segment:
                highest_segment = segment
                highest_file = filename

    return highest_file


def process_cdc_urls(csv_filename, warc_compressed, warc_extracted):
    """
    Process a list of URLs, download the closest WARC snapshot, 
    and extract resources.
    """
    with open(csv_filename, "r") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            url = row[0].strip()

            warc_filename = url.replace("https://www.cdc.gov/","") \
                               .rstrip("/*") \
                               .replace("/","_")
            logging.info(f"==================== NEW URL =============")
            logging.info(f"$warc_filename: {warc_filename}")
            logging.info(f"$url: {url}")
            logging.info(f"$warc_compressed: {warc_compressed}")
            warc_file = warc_compressed + warc_filename
            # Extract components from the WARC file
            logging.info(f"$warc_file:{warc_file}")
            
            # Download the WARC file for the best timestamp
            timestamp = get_best_date_for_url(url)
            download_warc_cdx_toolkit(url, timestamp, warc_file)

            #Once the download has been saved, select the file
            #with the highest number (most recent)
            warc_filename_toextract = get_highest_segment_file(
                                        warc_filename, 
                                        warc_compressed
                                        )
            if warc_filename_toextract is not None:
                warc_file_toextract = warc_compressed + warc_filename_toextract
                logging.info(f"$warc_file_toextract:{warc_file_toextract}")
                extract_warc_components(warc_file_toextract, warc_extracted)
