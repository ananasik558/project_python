import os
import pandas as pd
from datetime import datetime


def save_to_data_lake(data, source_type, folder_name):
    base_path = os.path.join("data_lake", folder_name)
    os.makedirs(base_path, exist_ok=True)

    current_date = datetime.now().strftime("%Y-%m-%d")
    date_folder = os.path.join(base_path, current_date)
    os.makedirs(date_folder, exist_ok=True)

    file_name = f"{source_type}_{current_date}.csv"
    file_path = os.path.join(date_folder, file_name)

    if isinstance(data, pd.DataFrame):
        data.to_csv(file_path, index=False)
    elif isinstance(data, dict):
        pd.DataFrame([data]).to_csv(file_path, index=False)
    else:
        raise ValueError("Unsupported data format.")

    print(f"Data saved to {file_path}")