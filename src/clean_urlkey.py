"""
Utility functions for processing archived URLs from the Wayback Machine.
"""

import os
import csv
import json
import logging

import requests
from urllib.parse import urlparse

def read_urls_from_csv(file_path):
    """
    Reads URLs from a CSV file and returns them as a list.
    Assumes URLs are in the first column.
    
    :param file_path: a single file path pointing to CSV file
    :return: List of URL strings
    """
    urls = []
    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Ensure row is not empty
                    urls.append(row[0].strip())  
        logging.debug(f"Successfully loaded {len(urls)} URLs from {file_path}")
    except FileNotFoundError:
        logging.critical(f"CSV file not found: {file_path}")
    except Exception as e:
        logging.critical(f"Error reading CSV file {file_path}: {e}")
    return urls

def clean_urls(url_headers, url_list):
    """
    Remove archival prefix (e.g., timestamps or metadata) before the close parenthesis ')'.
    
    :param url_list: A list of lists of URLs and timestamps
    :return: A list of cleaned URL paths.
    """
    headers = {}
    for ix, header in enumerate(url_headers):
        headers[header] = ix

    url_headers.append("path")
    headers['path'] = len(url_headers) - 1

    url_headers.append('originals')
    headers['originals'] = len(url_headers) - 1

    url_timestamps = {}
    url_collection = {}
    for url_data in url_list:
        raw_path = url_data[headers["urlkey"]]
        timestamp = url_data[headers["timestamp"]]
        try:
            if isinstance(raw_path, bytes):
                raw_path = raw_path.decode("utf-8")

            raw_path = raw_path.strip()

            if ')' in raw_path:
                _, path = raw_path.split(')', 1)
            else:
                print(f"raw_path = {raw_path}")
                path = raw_path

            url_data.append(path)
            original = url_data[headers['original']]
            if not (path in url_timestamps):
                # First time seeing this urlkey-path
                url_data.append([ original ])
                url_timestamps[path] = timestamp
                url_collection[path] = url_data
            elif timestamp > url_timestamps[path]:
                originals = url_collection[path][headers['originals']]
                logging.debug(f"Repeat of {path} - {original} vs {originals}")
                if not (original in originals):
                    originals.append(original)
                url_data.append(originals)
                url_timestamps[path] = timestamp
                url_collection[path] = url_data
        except (ValueError, UnicodeDecodeError) as e:
            logging.warning(f"Skipping malformed URL entry: {raw_path} — {e}")
        except Exception as e:
            logging.critical(f"Unexpected error cleaning URL {raw_path}: {e}")
    
    cleaned_paths = []
    for path, url_data in url_collection.items():
        cleaned_paths.append(dict(zip(url_headers, url_collection[path])))
    return cleaned_paths

def detect_urlkeys_from_subdomains(state_folder, subdomains):
    """
    Fetches URL keys from the Internet Archive's CDX API for a list of subdomains.

    :param state_folder: Folder in which to track/cache the list of URLs found on a previous run
    :param subdomains: List of subdomains (e.g., ["example.com", "blog.example.com"])
    :return: Dictionary {subdomain: set of urlkeys}
    """
    urlkeys = {}
    for sdomain in subdomains:
        parsed_url = urlparse(sdomain)
        netloc = parsed_url.netloc or parsed_url.path  # handle just subdomain strings

        # Check if we've already fetched this subdomain's list of URLs
        state_file = f"{state_folder}/url_list.{netloc}.list"
        if os.path.exists(state_file):
            with open(state_file, 'r', encoding='utf-8') as state_fd:
                cleaned_data = []
                for line in state_fd:
                    line = line.rstrip()
                    cleaned_data.append(json.loads(line))
                urlkeys[netloc] = cleaned_data
                state_fd.close()
            logging.info(f"Retrieved {len(urlkeys[netloc])} URLs for {netloc} from state file.")
            continue

        url = f"{netloc}/"

        cdx_call = (
            f"http://web.archive.org/cdx/search/cdx?"
            f"url={url}"
            f"&matchType=prefix"
            f"&from=20200101"
            f"&to=20250119"
            f"&filter=statuscode:200"
            f"&output=json"
        )

        try:
            response = requests.get(cdx_call)
            if response.status_code == 200:
                raw_data = json.loads(response.text)
                raw_headers = raw_data[0]
                raw_data = raw_data[1:]
                cleaned_data = clean_urls(raw_headers, raw_data)
                urlkeys[netloc] = cleaned_data

                # Preserve the list in the appropriate state_file
                with open(state_file, 'w', encoding='utf-8') as state_fd:
                    for url in cleaned_data:
                        state_fd.write(json.dumps(url) + "\n")
                    state_fd.close()
            else:
                logging.error(f"Error retrieving urlkeys for subdomain: {netloc} — Status code: {response.status_code}")
        except Exception as e:
            logging.exception(f"Exception retrieving urlkeys for subdomain: {netloc} — {str(e)}")
        logging.info(f"Retrieved {len(urlkeys[netloc])} URLs for {netloc}")
    return urlkeys
