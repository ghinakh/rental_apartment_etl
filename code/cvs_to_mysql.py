import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Read MySQL connection info
hostname = os.getenv("MYSQL_HOST")
port = int(os.getenv("MYSQL_PORT"))
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")

# csv path
base_dir = os.getcwd()
apart_path = os.path.join(base_dir, "data", "apartments.csv")
apart_att_path = os.path.join(base_dir, "data", "apartment_attributes.csv")

# read file
apart = pd.read_csv(apart_path)
apart_att = pd.read_csv(apart_att_path)

apart = apart.replace({np.nan: None})
apart_att = apart_att.replace({np.nan: None})

# table name
table_apart = "apartments"
table_att = "apartment_attributes"


def generate_insert_into(df, table_name="A"):
    columns = df.columns.tolist()
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_stmt = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"
    return insert_stmt
query_create_table_apart = """
CREATE TABLE apartments (
    id VARCHAR(255),
    title VARCHAR(255),
    source VARCHAR(255),
    price INT,
    currency VARCHAR(10),
    listing_created_on DATETIME,
    is_active TINYINT(1),
    last_modified_timestamp DATETIME
);
"""
query_create_table_att = """
CREATE TABLE apartment_attributes (
    id VARCHAR(255),
    category VARCHAR(255),
    body TEXT,
    amenities TEXT,
    bathrooms FLOAT,
    bedrooms FLOAT,
    fee FLOAT,
    has_photo VARCHAR(10),
    pets_allowed VARCHAR(10),
    price_display VARCHAR(255),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(50),
    latitude DOUBLE,
    longitude DOUBLE
);
"""
query_insert_table_apart = generate_insert_into(apart, 'apartments')
query_insert_table_att = generate_insert_into(apart_att, 'apartment_attributes')


try:
    connection = mysql.connector.connect(host=hostname,  user=username, password=password, port=port)
    if connection.is_connected():
        print("Connect to MySQL Server successfully!")

        cursor = connection.cursor()

        cursor.execute("CREATE DATABASE IF NOT EXISTS rental_apartment_app;")

        cursor.execute("USE rental_apartment_app;")
        
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print("You're connected to database: ", record[0])

        cursor.execute(f"DROP TABLE IF EXISTS {table_apart};")
        print(f"Table {table_apart} removed if exist")

        cursor.execute(f"DROP TABLE IF EXISTS {table_att};")
        print(f"Table {table_att} removed if exist")

        cursor.execute(query_create_table_apart)
        print(f"Table {table_apart} created successfully!")

        cursor.execute(query_create_table_att)
        print(f"Table {table_att} created successfully!")

        print(f"Apartments Count: {len(apart)}, Attributes Count: {len(apart_att)}")

        batch_size = 1000
        num_batches_apart = len(apart) // batch_size + 1

        for i in range(num_batches_apart):
            start_idx = i * batch_size
            end_idx = (i+1) *batch_size
            batch_apart = apart.iloc[start_idx:end_idx]
            batch_apart_record = [tuple(row) for row in batch_apart.to_numpy()]

            cursor.executemany(query_insert_table_apart, batch_apart_record)
            connection.commit()
            print(f"Batch {i+1}/{num_batches_apart} inserted successfully!")
        
        print(f"All {len(apart)} apartments data inserted successfully!")

        num_batches_att = len(apart_att) // batch_size + 1

        for i in range(num_batches_att):
            start_idx = i * batch_size
            end_idx = (i+1) *batch_size
            batch_att = apart_att.iloc[start_idx:end_idx]
            batch_att_record = [tuple(row) for row in batch_att.to_numpy()]

            cursor.executemany(query_insert_table_att, batch_att_record)
            connection.commit()
            print(f"Batch {i+1}/{num_batches_att} inserted successfully!")

        
        print(f"All {len(apart)} apartment attributes data inserted successfully!")
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


