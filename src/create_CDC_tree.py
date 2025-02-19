import os
import csv
import logging
from urllib.parse import urlparse

# Configure logging: logs to both console and a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("../logs/create_cdc_structure.log")  # Log to file
    ]
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
                    urls.append(row[0].strip())  # Read the first column (assuming it contains URLs)
        logging.info(f"Successfully loaded {len(urls)} URLs from {file_path}")
    except Exception as e:
        logging.critical(f"Error reading CSV file {file_path}: {e}")
    return urls

def process_urls(url_list, output_base):
    """
    Processes a list of URLs to extract directory paths,
    deduplicates them, and creates the corresponding directory structure.
    """
    dir_paths = set()
    logging.info("Starting URL processing...")

    for url in url_list:
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path  # Get path without domain
            # Remove the filename (if any) by splitting off the last component
            directories = path.rsplit("/", 1)[0]
            if directories:
                dir_paths.add(directories)
            logging.debug(f"Processed URL: {url} => {directories}")
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")

    # Sorting ensures that parent directories are created before any subdirectories.
    sorted_dirs = sorted(dir_paths)
    logging.info(f"Total unique directory paths found: {len(sorted_dirs)}")

    # Create directories
    for dir_path in sorted_dirs:
        full_path = os.path.join(output_base, dir_path.lstrip("/"))
        try:
            os.makedirs(full_path, exist_ok=True)
            logging.info(f"Created directory: {full_path}")
        except Exception as e:
            logging.error(f"Error creating directory {full_path}: {e}")

    logging.info("Directory structure creation completed.")

def create_cdc_tree(csv_file, output_base):
    """Main function to create the CDC directory tree from URLs in a CSV file.
    """
    try:
        urls = read_urls_from_csv(csv_file)
        if urls:
            process_urls(urls, output_base)
        else:
            logging.warning("No URLs were found in the CSV file.")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}")
