from google.cloud import bigquery, storage
import os
import pandas as pd
import gcsfs
import argparse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Inisialisasi argparse
parser = argparse.ArgumentParser()
parser.add_argument('--date_logic', required=True, help='Date Logic (format: YYYY-MM-DD)')
args = parser.parse_args()
date_logic = args.date_logic

# Storage
bucket_name = os.getenv("GCP_BUCKET")
fs = gcsfs.GCSFileSystem(token=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
bucket = client.bucket(bucket_name)

# Read parquet files from GCS
def read_parquet_gcs(path):
    return pd.read_parquet(f'gs://{bucket_name}/{path}', engine='pyarrow')
    # return pd.read_parquet(f'gs://{bucket_name}/{path}', filesystem=fs)

# read all .parquet files in GCS bucket BUCKET_NAME/rental_apartment_app/silver/*
apartment_cleaned = read_parquet_gcs('rental_apartment_app/silver/apartments_cleaned.parquet')
apartment_att_cleaned = read_parquet_gcs('rental_apartment_app/silver/apartment_attributes_cleaned.parquet')
user_viewings_cleaned = read_parquet_gcs('rental_apartment_app/silver/user_viewings_cleaned.parquet')
fulldetails_cleaned = read_parquet_gcs('rental_apartment_app/silver/rentapart_fulldetailed_cleaned.parquet')

kpi_apartment_performance = read_parquet_gcs(f'rental_apartment_app/silver/{date_logic}/kpi_apartment_performance.parquet')
kpi_hour_summary = read_parquet_gcs(f'rental_apartment_app/silver/{date_logic}/kpi_hour_summary.parquet')
kpi_dayofmonth_summary = read_parquet_gcs(f'rental_apartment_app/silver/{date_logic}/kpi_dayofmonth_summary.parquet')
kpi_state = read_parquet_gcs(f'rental_apartment_app/silver/{date_logic}/kpi_state.parquet')
kpi_platform = read_parquet_gcs(f'rental_apartment_app/silver/{date_logic}/kpi_platform.parquet')

# BigQuery
client = bigquery.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Buat ID dataset, pastikan sesuai format: project_id.dataset_id
PROJECT_ID = os.getenv('PROJECT_ID')
dataset_id = "rent_apart"

# initialization dataset
dataset = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
dataset.location = "US"

# create dataset
dataset = client.create_dataset(dataset, exists_ok=True)  # exists_ok=True biar nggak error kalau sudah ada
print(f"Dataset {dataset_id} successfully created or already existed")

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

# load fact dan dim
client.load_table_from_dataframe(apartment_cleaned, f'{PROJECT_ID}.{dataset_id}.dim_apartments', job_config=job_config).result()
client.load_table_from_dataframe(apartment_att_cleaned, f'{PROJECT_ID}.{dataset_id}.dim_apartmentattributes', job_config=job_config).result()
# client.load_table_from_dataframe(user_viewings_cleaned, f'{dataset_id}.fact_userviewings', job_config=job_config).result()
# client.load_table_from_dataframe(fulldetails_cleaned, f'{dataset_id}.fact_full_userviewings', job_config=job_config).result()

client.load_table_from_dataframe(user_viewings_cleaned, f'{PROJECT_ID}.{dataset_id}.fact_userviewings').result()
client.load_table_from_dataframe(fulldetails_cleaned, f'{PROJECT_ID}.{dataset_id}.fact_full_userviewings').result()

print("Upload fact and dimension tables to BigQuery success!")

# load kpi table
client.load_table_from_dataframe(kpi_apartment_performance, f'{dataset_id}.{date_logic}_kpi_apartment_performance', job_config=job_config).result()
client.load_table_from_dataframe(kpi_hour_summary, f'{dataset_id}.{date_logic}_kpi_hour_summary', job_config=job_config).result()
client.load_table_from_dataframe(kpi_dayofmonth_summary, f'{dataset_id}.{date_logic}_kpi_dayofmonth_summary', job_config=job_config).result()
client.load_table_from_dataframe(kpi_state, f'{dataset_id}.{date_logic}_kpi_state', job_config=job_config).result()
client.load_table_from_dataframe(kpi_platform, f'{dataset_id}.{date_logic}_kpi_platform', job_config=job_config).result()

print(f"Upload KPI tables for date: {date_logic} to BigQuery success!")