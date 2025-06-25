# cloud import
from pyspark.sql import SparkSession
database="rental_apartment_app"

# local development import
import os
base_dir = os.getcwd()
credential_path = os.path.join(base_dir, "service_account_gcp.json")
print(credential_path)

from dotenv import load_dotenv
load_dotenv()
bucket_name = os.getenv("GCP_BUCKET_NAME")

spark = (SparkSession.builder
         .appName('Local GCS Read')
         .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true')
         .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', credential_path)
         .getOrCreate())

print("Start Spark Session")

df_apart = spark.read.csv(f'gs://{bucket_name}/{database}/apartments/apartments_batch*.csv')
df_att = spark.read.csv(f'gs://{bucket_name}/{database}/apartment_attributes/apartment_attributes_batch*.csv')

print("Read Success")

df_apart_clean = df_apart.dropna()
df_att_clean = df_att.dropna()

print("Transform Success")

df_apart_clean.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/apartments/silver/apartments_cleaned.parquet')
df_att_clean.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/apartment_attributes/silver/apartment_attributes_cleaned.parquet')

print("Load Success")

spark.stop()
