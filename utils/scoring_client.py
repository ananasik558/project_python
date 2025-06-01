# utils/scoring_client.py

import requests

SCORING_SERVICE_URL = "http://scoring-assistant/api/v1/score"

def send_to_scoring_assistant(client_data):
    try:
        response = requests.post(SCORING_SERVICE_URL, json=client_data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при вызове скоринга: {e}")
        return None
