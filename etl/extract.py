import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

def extract_from_api(url, headers=None):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API: {response.status_code}")

def extract_from_csv(file_path):
    return pd.read_csv(file_path)

def extract_from_json(file_path):
    with open(file_path, 'r') as file:
        return pd.read_json(file)

def extract_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for item in root:
        row = {child.tag: child.text for child in item}
        data.append(row)
    return pd.DataFrame(data)


def extract_data_from_csv(directory):
    data_frames = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".csv"):
            file_path = os.path.join(directory, file_name)
            data = pd.read_csv(file_path)
            data_frames.append(data)
    return pd.concat(data_frames, ignore_index=True)