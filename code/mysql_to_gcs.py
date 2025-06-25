import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.cloud import storage
from dotenv import load_dotenv

# Load env
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


bucket_name = os.getenv("GCP_BUCKET_NAME")
print(bucket_name)
hostname = os.getenv("MYSQL_HOST")
port = int(os.getenv("MYSQL_PORT"))
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = "rental_apartment_app"

def upload_to_gcs(bucket_name, destination_blob_name, data_string):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_string)
    print(f"Uploaded to gs://bucket-secret-name/{destination_blob_name}")

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
        print("Connect to MySQL Server successfully!")
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print(f"You are connected to database: {record}")

        tables = ['apartments', 'apartment_attributes']


        for tbl in tables:
            # batch
            batch_size = 1000
            offset = 0
            suffix = 1
            while True:
                records = fetch_batch(tbl, batch_size, offset, cursor)
                if not records:
                    break
                
                df= pd.DataFrame(records, columns=cursor.column_names)
                csv_data = df.to_csv(index=False)

                upload_to_gcs(bucket_name, f"{database}/{tbl}/{tbl}_batch_{suffix}.csv", csv_data)

                print(f"Data {tbl}_batch_{str(suffix)} uploaded successfully!")
                offset += batch_size
                suffix += 1

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
