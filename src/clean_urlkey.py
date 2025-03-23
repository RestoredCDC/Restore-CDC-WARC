"""
Utility functions for processing archived URLs from the Wayback Machine.
"""

import os
import csv
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

def clean_urls(url_list):
    """
    Remove archival prefix (e.g., timestamps or metadata) before the close parenthesis ')'.
    
    :param url_list: A list of raw URL strings.
    :return: A set of cleaned URL paths.
    """
    cleaned_paths = set()
    for raw_path in url_list:
        try:
            if isinstance(raw_path, bytes):
                raw_path = raw_path.decode("utf-8")

            raw_path = raw_path.strip()

            if ')' in raw_path:
                _, path = raw_path.split(')', 1)
            else:
                path = raw_path

            cleaned_paths.add(path)
        except (ValueError, UnicodeDecodeError) as e:
            logging.warning(f"Skipping malformed URL entry: {raw_path} — {e}")
        except Exception as e:
            logging.critical(f"Unexpected error cleaning URL {raw_path}: {e}")
    
    return cleaned_paths

def detect_urlkeys_from_subdomains(subdomains):
    """
    Fetches URL keys from the Internet Archive's CDX API for a list of subdomains.

    :param subdomains: List of subdomains (e.g., ["example.com", "blog.example.com"])
    :return: Dictionary {subdomain: set of urlkeys}
    """
    urlkeys = {}
    for sdomain in subdomains:
        parsed_url = urlparse(sdomain)
        netloc = parsed_url.netloc or parsed_url.path  # handle just subdomain strings
        url = f"{netloc}/"

        cdx_call = (
            f"http://web.archive.org/cdx/search/cdx?"
            f"url={url}"
            f"&matchType=prefix"
            f"&from=20200101"
            f"&to=20250119"
            f"&filter=statuscode:200"
            f"&collapse=urlkey"
            f"&fl=urlkey"
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