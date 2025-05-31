import sqlite3
import os

DB_PATH = "data_warehouse.db"

def load_data(transformed_dfs):
    conn = sqlite3.connect(DB_PATH)

    transformed_dfs['persons'].to_sql('Persons', conn, if_exists='replace', index=False)
    transformed_dfs['financial_info'].to_sql('FinancialInfo', conn, if_exists='replace', index=False)
    transformed_dfs['activity_log'].to_sql('ActivityLog', conn, if_exists='replace', index=False)

    print("Данные успешно загружены в базу данных")
    conn.close()