import argparse
import logging
import os
from config_loader import load_config
from clean_urlkey import detect_urlkeys_from_subdomains
from clean_urlkey import read_urls_from_csv
from retrieve_snapshot import process_cdc_urls
from create_leveldb import create_db

# Configure logging: logs to both console and a file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("../logs/restoreCDCWarc.log")  # Log to file
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

    args = parser.parse_args()
    # Load config
    config = load_config()

    #TODO: code to create directories if they do not exist

    # Select config based on run_mode
    if args.run_mode in config:
        selected_config = config[args.run_mode]
        print(f"Running in {args.run_mode} mode with config: {selected_config}")
    else:
        print(f"Error: run_mode '{args.run_mode}' not found in config.yaml")

    subdomains = read_urls_from_csv(selected_config['csv_file'])
    url_list = detect_urlkeys_from_subdomains(subdomains)
    process_cdc_urls(url_list, selected_config['extraction_input_folder'])
    create_db(selected_config['extraction_input_folder'])


if __name__ == "__main__":
    main()
