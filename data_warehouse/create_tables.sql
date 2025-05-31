<<<<<<< HEAD
=======
CREATE TABLE customer_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    credit_card_id INT REFERENCES credit_cards(credit_card_id),
    ip_address_id INT REFERENCES ip_addresses(ip_address_id),
    password_id INT REFERENCES passwords(password_id),
    amount FLOAT,
    transaction_datetime TIMESTAMP
);

>>>>>>> adada636204e414c128110f201d57f2f4fc23745
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(255),
    date_of_birth DATE,
    age INT
);

CREATE TABLE credit_cards (
    credit_card_id SERIAL PRIMARY KEY,
<<<<<<< HEAD
    card_number VARCHAR(50),
=======
    card_number VARCHAR(20),
>>>>>>> adada636204e414c128110f201d57f2f4fc23745
    expiration_date VARCHAR(10),
    security_code VARCHAR(10)
);

CREATE TABLE ip_addresses (
    ip_address_id SERIAL PRIMARY KEY,
    ip_address VARCHAR(50)
);

CREATE TABLE passwords (
    password_id SERIAL PRIMARY KEY,
    password_hash VARCHAR(255)
<<<<<<< HEAD
);

CREATE TABLE customer_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    credit_card_id INT REFERENCES credit_cards(credit_card_id),
    ip_address_id INT REFERENCES ip_addresses(ip_address_id),
    password_id INT REFERENCES passwords(password_id),
    amount FLOAT,
    transaction_datetime TIMESTAMP
=======
>>>>>>> adada636204e414c128110f201d57f2f4fc23745
);