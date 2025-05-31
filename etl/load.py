from sqlalchemy import create_engine

# Настройки подключения к базе данных
DATABASE_URL = "postgresql://user:password@localhost:5432/bank_data"
engine = create_engine(DATABASE_URL)

def load_data(customers, credit_cards, ip_addresses, passwords, customer_transactions):
    """Загружает данные в таблицы."""
    # Загрузка таблицы customers
    customers.to_sql("customers", engine, if_exists="append", index=False)
    print("Data loaded into customers table.")

    # Загрузка таблицы credit_cards
    credit_cards.to_sql("credit_cards", engine, if_exists="append", index=False)
    print("Data loaded into credit_cards table.")

    # Загрузка таблицы ip_addresses
    ip_addresses.to_sql("ip_addresses", engine, if_exists="append", index=False)
    print("Data loaded into ip_addresses table.")

    # Загрузка таблицы passwords
    passwords.to_sql("passwords", engine, if_exists="append", index=False)
    print("Data loaded into passwords table.")

    # Загрузка таблицы customer_transactions
    customer_transactions.to_sql("customer_transactions", engine, if_exists="append", index=False)
    print("Data loaded into customer_transactions table.")