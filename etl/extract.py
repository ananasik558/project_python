import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

def extract_from_api(url, headers=None):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        df.insert(0, 'id', range(1, len(df) + 1))
        return df
    else:
        raise Exception(f"Failed to fetch data from API: {response.status_code}")

def extract_from_csv(file_path):
    df = pd.read_csv(file_path, encoding='utf-8', index_col=False)
    df.insert(0, 'id', range(1, len(df) + 1))
    return df

def extract_from_json(file_path):
    with open(file_path, 'r') as file:
        data = pd.read_json(file)
    data.insert(0, 'id', range(1, len(data) + 1))
    return data

def extract_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for item in root:
        row = {child.tag: child.text for child in item}
        data.append(row)
