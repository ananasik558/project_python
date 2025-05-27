import os
DATABASE_CONFIG = {
    'data_warehouse': {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'O-guruma',
        'database': 'bank_data'
    }
}

API_CONFIG = {
    'bank_api': {
        'url': 'https://api.bank.com/v1/transactions ',
        'api_key': 'your_api_key_here'
    },
    'credit_bureau_api': {
        'url': 'https://api.creditbureau.com/v1/reports ',
        'api_key': 'your_api_key_here'
    }
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_SOURCES_DIR = os.path.join(BASE_DIR, "../data_sources")