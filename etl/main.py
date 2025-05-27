import os
from etl.extract import (
    extract_from_api,
    extract_from_csv,
    extract_from_json,
    extract_from_xml
)
from etl.save_to_data_lake import save_to_data_lake
from config.config import API_CONFIG, DATA_SOURCES_DIR


def load_data_from_apis():
    for api_name, api_config in API_CONFIG.items():
        print(f"Loading data from {api_name} API...")
        try:
            url = api_config['url']
            headers = {'Authorization': f"Bearer {api_config['api_key']}"}
            data = extract_from_api(url, headers)
            save_to_data_lake(data, api_name, api_name)
        except Exception as e:
            print(f"Failed to load data from {api_name} API: {e}")


def load_data_from_files():
    if not os.path.exists(DATA_SOURCES_DIR):
        print(f"Directory {DATA_SOURCES_DIR} does not exist.")
        return

    print("Loading data from files...")
    for file_name in os.listdir(DATA_SOURCES_DIR):
        file_path = os.path.join(DATA_SOURCES_DIR, file_name)
        if os.path.isfile(file_path):
            try:
                if file_name.endswith(".csv"):
                    print(f"Processing CSV file: {file_name}")
                    data = extract_from_csv(file_path)
                    source_type = "csv_data"
                elif file_name.endswith(".json"):
                    print(f"Processing JSON file: {file_name}")
                    data = extract_from_json(file_path)
                    source_type = "json_data"
                elif file_name.endswith(".xml"):
                    print(f"Processing XML file: {file_name}")
                    data = extract_from_xml(file_path)
                    source_type = "xml_data"
                else:
                    print(f"Unsupported file type: {file_name}")
                    continue

                save_to_data_lake(data, source_type, source_type)
            except Exception as e:
                print(f"Failed to process file {file_name}: {e}")


if __name__ == "__main__":
    load_data_from_apis()
    load_data_from_files()