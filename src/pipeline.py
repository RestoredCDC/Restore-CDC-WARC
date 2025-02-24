import argparse
from config_loader import load_config
#from create_CDC_tree import create_cdc_tree
#from create_CDC_tree import process_urls
from create_CDC_tree import create_data_dir
#from extraction_processing import extraction_tree
from retrieve_snapshot import process_cdc_urls
from process_html import process_directory
import logging

# Configure logging: logs to both console and a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s -  %(funcName)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler("../logs/download_warc.log")  # Log to file
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

    # Select config based on run_mode
    if args.run_mode in config:
        selected_config = config[args.run_mode]
        print(f"Running in {args.run_mode} mode with config: {selected_config}")
    else:
        print(f"Error: run_mode '{args.run_mode}' not found in config.yaml")


    # Change this to match your actual CSV file name
    #This call removed due to warcat creation of directories
    #create_cdc_tree(selected_config['csv_file'], 
    #                selected_config['output_base'])
    #process_urls(selected_config['csv_file'], 
    #                selected_config['warc_compressed'])
    
    create_data_dir(selected_config['warc_compressed'], 
                    selected_config['warc_extracted'])
    
    warc_file = process_cdc_urls(selected_config['csv_file'], 
                                 selected_config['warc_compressed'],
                                 selected_config['warc_extracted']) 
                    
    #warc_dir = warc_file.rsplit("/", 1)[0] + "/"
    
    #process_directory(warc_dir,  selected_config['warc_extracted']) 


if __name__ == "__main__":
    main()
