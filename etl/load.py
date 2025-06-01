import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from config.config import DATABASE_CONFIG
#1
def connect_to_db():
    """Подключается к базе данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
            database=DATABASE_CONFIG['database']
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def bulk_insert(conn, table_name, data):
    """
    Выполняет массовую вставку данных в таблицу.
    :param conn: Подключение к базе данных.
    :param table_name: Название таблицы.
    :param data: DataFrame с данными.
    """
    cursor = conn.cursor()
    columns = data.columns.tolist()
    values_placeholder = ', '.join(['%s'] * len(columns))
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(values_placeholder)
    )
    # Преобразуем DataFrame в список кортежей
    data_tuples = [tuple(row) for row in data.to_numpy()]
    try:
        cursor.executemany(insert_query, data_tuples)
        conn.commit()
    except Exception as e:
        print(f"Ошибка при вставке данных в таблицу {table_name}: {e}")
        conn.rollback()

def load_data_from_csv_to_db(base_path):
    """Загружает данные из всех CSV-файлов в базу данных."""
    conn = connect_to_db()
    if not conn:
        return

    for root, _, files in os.walk(base_path):
        for file_name in files:
            if file_name.endswith(".csv"):
                file_path = os.path.join(root, file_name)
                print(f"Processing file: {file_path}")
                try:
                    df = pd.read_csv(file_path)

                    # Переименовываем столбцы для соответствия базе данных
                    df = df.rename(columns={
                        'dob': 'date_of_birth',  # Переименовываем dob в date_of_birth
                        'credit_card_number': 'card_number',  # Переименовываем credit_card_number в card_number
                        'credit_card_expiration': 'expiration_date',
                        'credit_card_security_code': 'security_code'
                    })

                    # Разделяем данные по таблицам
                    customers = df[['full_name', 'phone', 'email', 'date_of_birth', 'age']]
                    credit_cards = df[['card_number', 'expiration_date', 'security_code']]
                    ip_addresses = df[['ipv4']].rename(columns={'ipv4': 'ip_address'})
                    passwords = df[['password']].rename(columns={'password': 'password_hash'})

                    # Массовая вставка данных
                    bulk_insert(conn, 'customers', customers)
                    bulk_insert(conn, 'credit_cards', credit_cards)
                    bulk_insert(conn, 'ip_addresses', ip_addresses)
                    bulk_insert(conn, 'passwords', passwords)

                    print(f"Data from {file_name} loaded successfully.")
                except Exception as e:
                    print(f"Ошибка при обработке файла {file_name}: {e}")
    conn.close()

if __name__ == "__main__":
    base_path = "../data_lake"
    load_data_from_csv_to_db(base_path)