import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from config.config import DATABASE_CONFIG

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

def insert_or_ignore(conn, table_name, data, unique_columns):
    cursor = conn.cursor()
    for _, row in data.iterrows():
        columns = list(row.index)
        values = [row[col] for col in columns]
        unique_conditions = [f"{col} = %s" for col in unique_columns]
        check_query = sql.SQL("SELECT 1 FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            sql.SQL(" AND ").join(map(sql.SQL, unique_conditions))
        )
        cursor.execute(check_query, [row[col] for col in unique_columns])
        exists = cursor.fetchone()

        if not exists:
            insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns))
            )
            cursor.execute(insert_query, values)
    conn.commit()

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
                df = pd.read_csv(file_path)

                # Преобразуем данные
                df['dob'] = pd.to_datetime(df['dob'], errors='coerce').dt.strftime('%Y-%m-%d')  # Преобразуем дату
                df = df.rename(columns={
                    'dob': 'date_of_birth',
                    'credit_card_number': 'card_number',
                    'credit_card_expiration': 'expiration_date',
                    'credit_card_security_code': 'security_code'
                })

                # Разделяем данные по таблицам
                customers = df[['full_name', 'phone', 'email', 'date_of_birth', 'age']].drop_duplicates()
                credit_cards = df[['card_number', 'expiration_date', 'security_code']].drop_duplicates()
                ip_addresses = df[['ipv4']].drop_duplicates().rename(columns={'ipv4': 'ip_address'})
                passwords = df[['password']].drop_duplicates().rename(columns={'password': 'password_hash'})

                # Проверяем наличие обязательных столбцов
                required_columns_credit_cards = {'card_number', 'expiration_date', 'security_code'}
                if not required_columns_credit_cards.issubset(credit_cards.columns):
                    print(f"Ошибка: Не все необходимые столбцы для таблицы credit_cards найдены. Пропущенные столбцы: "
                          f"{required_columns_credit_cards - set(credit_cards.columns)}")
                    continue

                # Вставляем данные в таблицы
                insert_or_ignore(conn, 'customers', customers, ['email'])
                insert_or_ignore(conn, 'credit_cards', credit_cards, ['card_number'])
                insert_or_ignore(conn, 'ip_addresses', ip_addresses, ['ip_address'])
                insert_or_ignore(conn, 'passwords', passwords, ['password_hash'])

                print(f"Data from {file_name} loaded successfully.")
    conn.close()

if __name__ == "__main__":
    base_path = "../data_lake"
    load_data_from_csv_to_db(base_path)
