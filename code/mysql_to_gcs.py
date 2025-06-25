import os
import time
import pandas as pd
import mysql.connector
from mysql.connector import Error
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError, GoogleAuthError
from dotenv import load_dotenv

# Load env
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


bucket_name = os.getenv("GCP_BUCKET_NAME")
hostname = os.getenv("MYSQL_HOST")
port = int(os.getenv("MYSQL_PORT"))
username = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = "rental_apartment_app"

def check_gcs_credentials(bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        print(f"GCS credential valid. Bucket '{bucket_name}' is accessible.")
        return True
    except DefaultCredentialsError:
        print("Google Cloud credential not found. Please set GOOGLE_APPLICATION_CREDENTIALS properly.")
        return False
    except GoogleAuthError as e:
        print(f"Authentication error: {e}")
        return False
    except Exception as e:
        print(f"Failed to access GCS: {e}")
        return False

def upload_to_gcs(bucket_name, destination_blob_name, data_string):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_string)
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
        print("Connect to MySQL Server successfully!")
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print(f"You are connected to database: {record[0]}")

        tables = ['apartments', 'apartment_attributes']

        if not check_gcs_credentials(bucket_name):
            print("Terminating process due to GCS credential issue.")
            connection.close()
            exit()

        for tbl in tables:
            # batch
            batch_size = 1000
            offset = 0
            suffix = 1
            while True:
                start = time.time()
                records = fetch_batch(tbl, batch_size, offset, cursor)
                if not records:
                    break
                
                df= pd.DataFrame(records, columns=cursor.column_names)
                csv_data = df.to_csv(index=False)

                upload_to_gcs(bucket_name, f"{database}/{tbl}/{tbl}_batch_{suffix}.csv", csv_data)

                end = time.time()
                print(f"[Executed Time: {end - start} seconds] Data {tbl}_batch_{str(suffix)} uploaded to GCS successfully!")
                offset += batch_size
                suffix += 1


except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
