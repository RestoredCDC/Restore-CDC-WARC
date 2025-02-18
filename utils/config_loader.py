import yaml

# Load configuration
def load_config(file_path="../run_config.yaml"):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)  # Use safe_load to avoid security risks
    return config