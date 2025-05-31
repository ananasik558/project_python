import pandas as pd

def transform_data(data):
    """Преобразует данные для загрузки в таблицы."""
    # Таблица customers
    customers = data[["full_name", "phone", "email", "dob", "age"]]
    customers.drop_duplicates(inplace=True)

    # Таблица credit_cards
    credit_cards = data[["credit_card_number", "credit_card_expiration", "credit_card_security_code"]]
    credit_cards.drop_duplicates(inplace=True)

    # Таблица ip_addresses
    ip_addresses = data[["ipv4"]]
    ip_addresses.columns = ["ip_address"]
    ip_addresses.drop_duplicates(inplace=True)

    # Таблица passwords
    passwords = data[["password"]]
    passwords.columns = ["password_hash"]
    passwords.drop_duplicates(inplace=True)

    # Таблица customer_transactions
    customer_transactions = data[[
        "random_float", "random_datetime", "full_name", "ipv4", "password", "credit_card_number"
    ]]
    customer_transactions.columns = [
        "amount", "transaction_datetime", "full_name", "ip_address", "password_hash", "card_number"
    ]

    return customers, credit_cards, ip_addresses, passwords, customer_transactions