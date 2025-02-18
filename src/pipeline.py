import argparse
from utils.config_loader import load_config
from create_CDC_tree import create_cdc_tree
from extraction_processing import extraction_processing
from retrieve_snapshot import process_cdc_urls
from process_html import process_directory


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
    create_cdc_tree(selected_config['csv_file'], selected_config['output_base'])
    process_cdc_urls(selected_config['csv_file'], selected_config['output_base']) #retrieve snapshots
    extraction_processing(selected_config['extraction_input_folder'], selected_config['extraction_output_folder'])
    process_directory(selected_config['extraction_output_folder'], selected_config['extraction_output_folder']) #process html files


if __name__ == "__main__":
    main()
