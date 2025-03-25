#!/usr/bin/env python3

import argparse
import logging
from pathlib import Path
from sys import exit
from time import time

from clean_urlkey import detect_urlkeys_from_subdomains, read_urls_from_csv
from config_loader import load_config
from retrieve_snapshot import process_cdc_urls
from create_leveldb import create_db

# Logging directory setup
LOG_DIR = Path("../logs")
if not LOG_DIR.exists():
    logging.info(f"Creating log directory: {LOG_DIR}")
    LOG_DIR.mkdir(mode=0o755, parents=True)

# Configure logging: logs to both console and a file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "restoreCDCWarc.log")
    ]
)

def main():
    parser = argparse.ArgumentParser(description="Select run mode for configuration")
    parser.add_argument(
        "--run_mode",
        choices=["dev", "prod"],
        default="dev",
        help="Specify the run mode: 'dev' or 'prod'"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Then set log level dynamically:
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.getLogger().setLevel(log_level)

    config = load_config()
    
    if args.run_mode in config:
        selected_config = config[args.run_mode]
        logging.info(f"Running in {args.run_mode} mode with config: {selected_config}")
    else:
        logging.error(f"run_mode '{args.run_mode}' not found in config.yaml")
        exit(1)

    required_keys = [
        'csv_file', 'warc_folder', 'db_folder',
        'track_failed_urls', 'new_failed_url_list', 'failed_url_list'
    ]
    for key in required_keys:
        if key not in selected_config:
            logging.error(f"Missing required config key: {key}")
            exit(1)

    csv_path = Path(selected_config['csv_file'])
    if not csv_path.exists():
        logging.error(f"CSV file not found: {csv_path}")
        exit(1)

    folders = [
        Path(selected_config['warc_folder']),
        Path(selected_config['db_folder']),
        Path(selected_config['state_folder'])
    ]
    for folder in folders:
        if folder.exists():
            continue
        logging.info(f"Creating directory: {folder}")
        folder.mkdir(mode=0o755, parents=True)

    start_time = time()

    #subdomains = read_urls_from_csv(selected_config['csv_file'])
    #url_list = detect_urlkeys_from_subdomains(selected_config['state_folder'], subdomains)

    logging.debug("Starting process_cdc_urls")
    #failed_urls = process_cdc_urls(
    #    selected_config['state_folder'],
    #    selected_config['warc_folder'],
    #    selected_config['track_failed_urls'],
    #    selected_config['failed_url_list'],
    #    url_list
    #)

    #if selected_config.get("track_failed_urls") and len(failed_urls) > 0:
    #    with open(selected_config["failed_url_list"], "w") as f:
    #        for url in failed_urls:
    #            f.write(url + "\n")
    #    logging.info(f"Saved failed URLs to {selected_config['failed_url_list']}")

    logging.debug("Starting create_db")
    create_db(
        selected_config['warc_folder'],
        selected_config['db_folder']
    )
    
    duration = time() - start_time
    logging.info(f"Script completed in {duration:.2f} seconds")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Unhandled exception: {e}")
        exit(1)
