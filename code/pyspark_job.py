# cloud import
from pyspark.sql import SparkSession
database="rental_apartment_app"


spark = SparkSession.builder.appName('rental_apart_app').getOrCreate()

bucket_name = ''
df_apart = spark.read.csv(f'gs://{bucket_name}/{database}/apartments/apartments_batch*.csv')
df_att = spark.read.csv(f'gs://{bucket_name}/{database}/apartment_attributes/apartment_attributes_batch*.csv')



# df_apart_clean.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/apartments/silver/apartments_cleaned.parquet')
# df_att_clean.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/apartment_attributes/silver/apartment_attributes_cleaned.parquet')

spark.stop()
