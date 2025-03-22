import os
import csv
import logging
import requests
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
)

def read_urls_from_csv(file_path):
    """
    Reads URLs from a CSV file and returns them as a list.
    Assumes URLs are in the first column.
    """
    urls = []
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Ensure row is not empty
                    urls.append(row[0].strip())  # Read the first column 
                                                 #(assuming it contains URLs)
        logging.debug(f"Successfully loaded {len(urls)} URLs from {file_path}")
    except Exception as e:
        logging.critical(f"Error reading CSV file {file_path}: {e}")
    return urls

def clean_urls(url_list):
    cleaned_paths = set()
    for raw_path in url_list:
        try:
            if isinstance(raw_path, bytes):
                raw_path = raw_path.decode('utf-8')
            raw_path = raw_path.strip()

            # Remove the subdomain prefix
            if ')' in raw_path:
                path = raw_path.split(')', 1)[1]
                cleaned_paths.add(path)
            else:
                path = raw_path
                cleaned_paths.add(path)
        except Exception as e:
            logging.critical(f"Error cleaning URL {raw_path}: {e}") 
    
    return cleaned_paths

def detect_urlkeys_from_subdomains(subdomains):
    """
    Takes list of subdomains, pulls all archived page paths and sends them back
    """
    urlkeys = {}
    for sdomain in subdomains:
        parsed_url = urlparse(sdomain)
        netloc = parsed_url.netloc or parsed_url.path  # handle just subdomain strings
        url = f"{netloc}/"

        cdx_call = (
            f"http://web.archive.org/cdx/search/cdx?"
            f"url={url}&matchType=prefix&from=20240101&to=20250119&"
            f"filter=statuscode:200&collapse=urlkey&fl=urlkey"
        )

        try:
            response = requests.get(cdx_call)
            if response.status_code == 200:
                raw_data = response.text.splitlines()
                cleaned_data = clean_urls(raw_data)
                urlkeys[netloc] = cleaned_data
            else:
                logging.error(f"Error retrieving urlkeys for subdomain: {netloc} — Status code: {response.status_code}")
        except Exception as e:
            logging.exception(f"Exception retrieving urlkeys for subdomain: {netloc} — {str(e)}")

    return urlkeys