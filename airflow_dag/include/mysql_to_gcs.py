import os
import time
import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

# MySQL configuration
hostname = os.getenv("MYSQL_HOST")
port = int(os.getenv("MYSQL_PORT"))
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = "rental_apartment_app"

# GCS configuration
bucket_name = os.getenv("GCP_BUCKET")

def upload_to_gcs(bucket_name, destination_blob_name, local_file):
    client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file)
    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")

def fetch_batch(table_name, batch_size, offset, cursor):
    query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
    cursor.execute(query)
    records = cursor.fetchall()
    return records

try:
    connection = mysql.connector.connect(
        host=hostname,
        user=username,
        password=password,
        port=port,
        database=database
    )

    if connection.is_connected():
        print("Connected to MySQL Server successfully!")

        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        print(f"You're connected to database: {record['DATABASE()']}")

        tables = ['apartments', 'apartment_attributes']

        client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        bucket = client.bucket(bucket_name)

        for tbl in tables:
            batch_size = 1000
            offset = 0
            suffix = 1
            while True:
                start = time.time()
                records = fetch_batch(tbl, batch_size, offset, cursor)
                if not records:
                    break

                df = pd.DataFrame(records)

                # Save batch as JSON
                local_filename = f"{tbl}_batch_{suffix}.json"
                gcs_path = f"{database}/{tbl}/{tbl}_batch_{suffix}.json"

                df.to_json(local_filename, orient='records', lines=True, date_format='iso')
                print(f"Batch {suffix} saved locally: {local_filename}")
            
                # Upload to GCS
                upload_to_gcs(bucket_name, gcs_path, local_filename)

                # Delete local file
                os.remove(local_filename)
                print(f"Local file {local_filename} deleted successfully.")

                end = time.time()
                print(f"[Executed Time: {end - start} seconds] Batch {suffix} uploaded to GCS successfully!")

                offset += batch_size
                suffix += 1
                

except Error as e:
    print("Error while connecting to MySQL:", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")