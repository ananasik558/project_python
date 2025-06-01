import pandas as pd
from pathlib import Path

DATA_PATH = Path("../data_lake/csv_data/2025-06-01") / "csv_data_2025-06-01.csv"

def load_client_data():
    """Загружает данные клиентов из CSV"""
    return pd.read_csv(DATA_PATH, delimiter='\t')
