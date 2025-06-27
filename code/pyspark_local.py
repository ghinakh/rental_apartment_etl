# cloud import
from pyspark.sql import SparkSession
database="rental_apartment_app"
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, TimestampType, DateType, NumericType, BooleanType
from pyspark.sql.window import Window

# local development import
import os
base_dir = os.getcwd()
credential_path = os.path.join(base_dir, "service_account_gcp.json")
print(credential_path)

from dotenv import load_dotenv
load_dotenv()
bucket_name = os.getenv("GCP_BUCKET_NAME")

import pandas as pd

def check_handle_timestamp_pyspark(df, timestamp_cols):
    # List of supported formats: regex + parsing format
    formats = [
        {
            "regex": r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) (0\d|1\d|2[0-3]):([0-5]\d):([0-5]\d)$',
            "format": "yyyy-MM-dd HH:mm:ss"
        },
        {
            "regex": r'^(0[1-9]|[12]\d|3[01])-(0[1-9]|1[0-2])-\d{4} (0\d|1\d|2[0-3]):([0-5]\d):([0-5]\d)$',
            "format": "dd-MM-yyyy HH:mm:ss"
        },
        {
            "regex": r'^\d{4}/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01]) (0\d|1\d|2[0-3]):([0-5]\d):([0-5]\d)$',
            "format": "yyyy/MM/dd HH:mm:ss"
        },
        {
            "regex": r'^(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/\d{4} (0\d|1\d|2[0-3]):([0-5]\d):([0-5]\d)$',
            "format": "dd/MM/yyyy HH:mm:ss"
        },
        {
            "regex": r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',
            "format": "yyyy-MM-dd'T'HH:mm:ss"
        }
    ]

    for col_name in timestamp_cols:
        print(f"Checking timestamp formats in column {col_name}...")

        expr = None
        for f in formats:
            regex = f["regex"]
            fmt = f["format"]

            if expr is None:
                expr = when(col(col_name).rlike(regex), to_timestamp(col(col_name), fmt))
            else:
                expr = expr.when(col(col_name).rlike(regex), to_timestamp(col(col_name), fmt))

        # Fallback jika tidak cocok format apapun
        expr = expr.otherwise(None)

        # Buat kolom baru yang sudah diparse
        df = df.withColumn(col_name, expr)

        # Cek apakah masih ada data yang tidak bisa diparse (jadi null)
        invalid_count = df.filter(col(col_name).isNull()).count()

        if invalid_count > 0:
            print(f"[WARNING] Found {invalid_count} invalid timestamp(s) in column {col_name}.")
        else:
            print(f"[INFO] All values in column {col_name} are in valid timestamp format.")

    return df

def check_handling_nan_pyspark(df, name):
    # Check if there are any nulls in the DataFrame
    any_nan = df.select([col(c).isNull().alias(c) for c in df.columns]).agg(
        *[sum(col(c).cast("int")).alias(c) for c in df.columns]
    ).toPandas().sum(axis=1)[0] > 0

    if any_nan:
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType

            total_count = df.count()
            null_count = df.filter(col(col_name).isNull()).count()

            if null_count == total_count:
                print(f"Column {col_name} in df {name} is fully null -> Dropping column...")
                df = df.drop(col_name)
                continue  # Langsung lanjut ke kolom berikutnya

            if null_count > 0:
                if isinstance(col_type, BooleanType):
                    print(f"Missing values on df {name}, column {col_name} (BooleanType) -> No action taken.")
                    continue  # Skip handling for boolean columns

                print(f"Handle missing values on df {name}, column {col_name}")

                if isinstance(col_type, StringType):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit('Unknown')).otherwise(col(col_name)))
                elif isinstance(col_type, TimestampType):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit('9999-12-31 23:59:59')).otherwise(col(col_name)))
                elif isinstance(col_type, DateType):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit('9999-12-31')).otherwise(col(col_name)))
                elif isinstance(col_type, NumericType):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
                else:
                    # Fallback untuk tipe yang tidak terdefinisi
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit('Unknown')).otherwise(col(col_name)))
            else:
                print(f"No missing values on df {name}, column {col_name}")
    else:
        print(f"No missing values found in any columns of df {name}")

    return df

def check_handling_duplicate_pyspark(df, id_column, name):
    # Hitung jumlah total rows dan jumlah unique rows berdasarkan id_column
    total_count = df.count()
    distinct_count = df.select(id_column).distinct().count()

    if total_count > distinct_count:
        print(f"Found duplicate on {name}, drop duplicate...")
        df = df.dropDuplicates([id_column])
    else:
        print(f"No duplicate on {name}")

    return df

spark = (SparkSession.builder
         .appName('Local GCS Read')
         .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true')
         .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', credential_path)
         .getOrCreate())

print("Start Spark Session")

user_view = pd.read_csv(os.path.join(base_dir, "data", "user_viewings.csv"))

# define schema
schema_userview = 'user_id STRING, apartment_id STRING, viewed_at STRING, is_wishlisted STRING, call_to_action STRING'
schema_apart = 'id STRING, title STRING, source STRING, price INT, currency STRING, listing_created_on TIMESTAMP, is_active TINYINT, last_modified_timestamp TIMESTAMP'
schema_att = 'id STRING, category STRING, body STRING, amenities STRING, bathrooms FLOAT, bedrooms FLOAT, fee FLOAT, has_photo STRING, pets_allowed STRING, price_display STRING, price_type STRING, square_feet INT, address STRING, cityname STRING, state STRING, latitude DOUBLE, longitude DOUBLE'

df_apart = spark.read.json(f'gs://{bucket_name}/{database}/apartments/apartments_batch*.json', schema=schema_apart)
df_att = spark.read.json(f'gs://{bucket_name}/{database}/apartment_attributes/apartment_attributes_batch*.json', schema=schema_att)
df_userview = spark.createDataFrame(user_view, schema=schema_userview)

df_apart = df_apart.cache()
df_att = df_att.cache()
df_userview = df_userview.cache()

print("Read Success")

# DF USER VIEWINGS
timestamp_cols = [
    "viewed_at"
]
df_userview = check_handle_timestamp_pyspark(df_userview, timestamp_cols)
df_userview = df_userview.withColumn("is_wishlisted", col('is_wishlisted').cast('boolean'))
df_userview = check_handling_nan_pyspark(df_userview, "User Viewings")
df_userview = check_handling_duplicate_pyspark(df_userview, ["user_id","apartment_id"], "User Viewings")
fact_userview = df_userview.select("*")
fact_userview.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/user_viewings_cleaned.parquet')
df_userview.unpersist()

# DF APARTMENTS
timestamp_cols = [
    "listing_created_on",
    "last_modified_timestamp"
]
df_apart = check_handle_timestamp_pyspark(df_apart, timestamp_cols)
df_apart = df_apart.withColumn("is_active", col('is_active').cast('boolean'))
df_apart = check_handling_nan_pyspark(df_apart, "Apartment")
df_apart = check_handling_duplicate_pyspark(df_apart, ["id"], "Apartment")
dim_apart = df_apart.select("*")
dim_apart.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/apartments_cleaned.parquet')
df_apart.unpersist()

# DF APARTMENT ATTRIBUTES
df_att = check_handling_nan_pyspark(df_att, "Apartment Attributes")
df_att = check_handling_duplicate_pyspark(df_att, ["id"], "Apartment Attributes")
dim_att = df_att.select("*")
dim_att.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/apartment_attributes_cleaned.parquet')
df_att.unpersist()

# JOIN
df_apart_att = dim_apart.join(dim_att, "id", 'left')
df_full = fact_userview.join(df_apart_att, fact_userview.apartment_id == df_apart_att.id, 'left')
df_full = df_full.cache()

# APARTMENT PERFORMANCE METRIC
user_engagement_per_apart = fact_userview.groupBy("apartment_id").agg(
    count("*").alias("total_views"),
    count(when(col("is_wishlisted") == True, 1)).alias("total_wishlists"),
    count(when(col("call_to_action") == "contact_agent", 1)).alias("total_contact_agent")
)
df_apartment_perf = df_apart_att.join(user_engagement_per_apart, df_apart_att.id == user_engagement_per_apart.apartment_id, 'left').drop("apartment_id")
df_apartment_perf = df_apartment_perf.select(
    col("id").alias("apartment_id"),
    col("title"),
    col("category"),
    col("cityname"),
    col("state"),
    col("source"),
    col("price"),
    col("currency"),
    col("is_active"),
    col("listing_created_on"),
    col("last_modified_timestamp"),
    col("total_views"),
    col("total_wishlists"),
    col("total_contact_agent")
)

# HOUR SUMMARY KPI
df_full = df_full.withColumn("view_hour", hour(col("viewed_at")))
df_full = df_full.withColumn("view_day", dayofmonth(col("viewed_at")))
df_full = df_full.withColumn("view_month", month(col("viewed_at")))
df_full = df_full.withColumn("view_year", year(col("viewed_at")))

df_hour_kpi = df_full.groupBy("view_hour").agg(
    count("*").alias("total_views"),
    count(when(col("is_wishlisted") == True, 1)).alias("total_wishlists"),
    count(when(col("call_to_action") == "contact_agent", 1)).alias("total_contact_agent")
)
agg_views = df_full.groupBy("view_hour", "source").agg(
    count("*").alias("total_views")
).orderBy("view_hour", "total_views", ascending=[True, False])

window_spec = Window.partitionBy("view_hour").orderBy(col("total_views").desc())
ranked_views = agg_views.withColumn("row_num", row_number().over(window_spec))
most_viewed_platform_per_hour = ranked_views.filter(col("row_num") == 1).drop("row_num")
df_hour_kpi = df_hour_kpi.join(most_viewed_platform_per_hour.select("view_hour","source"), on="view_hour", how="left")
df_hour_kpi = df_hour_kpi.withColumnRenamed("source", "most_viewed_platform")
agg_apart_views = df_full.groupBy("view_hour", "apartment_id").agg(
    count("*").alias("total_views")
).orderBy("view_hour", "total_views", ascending=[True, False])
top3_apart_views_per_hour = agg_apart_views.withColumn("rank", rank().over(window_spec)).withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") <= 3)
top3_apart_views_per_hour = top3_apart_views_per_hour.groupBy("view_hour").agg(concat_ws(", ", collect_list("apartment_id")).alias("top_apartments"))
df_hour_kpi = df_hour_kpi.join(top3_apart_views_per_hour, on="view_hour", how="left")
engagement_rate_per_hour = df_full.groupBy("view_hour").agg(
    round((sum(when(col("call_to_action") == "contact_agent", 1)) / count("*") * 100),2).alias("engagement_rate")
)
df_hour_kpi = df_hour_kpi.join(engagement_rate_per_hour, on="view_hour", how="left")

# DAYOFMONTH SUMMARY KPI
df_full = df_full.withColumn("is_weekend", when((dayofweek(col("viewed_at")) == 1) | (dayofweek(col("viewed_at")) == 7), True).otherwise(False))
df_dom_kpi = df_full.groupBy("view_day").agg(
    count("*").alias("total_views"),
    count(when(col("is_wishlisted") == True, 1)).alias("total_wishlists"),
    count(when(col("call_to_action") == "contact_agent", 1)).alias("total_contact_agent")
)
agg_views = df_full.groupBy("view_day", "source").agg(
    count("*").alias("total_views")
).orderBy("view_day", "total_views", ascending=[True, False])

window_spec = Window.partitionBy("view_day").orderBy(col("total_views").desc())
ranked_views = agg_views.withColumn("row_num", row_number().over(window_spec))
most_viewed_platform_per_dom = ranked_views.filter(col("row_num") == 1).drop("row_num")
df_dom_kpi = df_dom_kpi.join(most_viewed_platform_per_dom.select("view_day","source"), on="view_day", how="left")
df_dom_kpi = df_dom_kpi.withColumnRenamed("source", "most_viewed_platform")
agg_apart_views = df_full.groupBy("view_day", "apartment_id").agg(
    count("*").alias("total_views")
).orderBy("view_day", "total_views", ascending=[True, False])

top3_apart_views_per_dom = agg_apart_views.withColumn("rank", rank().over(window_spec)).withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") <= 3)
top3_apart_views_per_dom = top3_apart_views_per_dom.groupBy("view_day").agg(concat_ws(", ", collect_list("apartment_id")).alias("top_apartments"))
df_dom_kpi = df_dom_kpi.join(top3_apart_views_per_dom, on="view_day", how="left")
engagement_rate_per_dom = df_full.groupBy("view_day").agg(
    round((sum(when(col("call_to_action") == "contact_agent", 1)) / count("*") * 100),2).alias("engagement_rate")
)

df_dom_kpi = df_dom_kpi.join(engagement_rate_per_dom, on="view_day", how="left")

# STATE KPI
df_state_kpi = df_full.groupBy("state", "cityname").agg(
    count("*").alias("total_views"),
    count(when(col("is_wishlisted") == True, 1)).alias("total_wishlists"),
    count(when(col("call_to_action") == "contact_agent", 1)).alias("total_contact_agent"),
    min(col("price")).alias("min_price"),
    max(col("price")).alias("max_price"),
    count(when(col("is_active") == True, 1)).alias("total_active_apartment"),
    round(avg(col("price")),2).alias("avg_price"),
).withColumn("wishlist_rate", round((col("total_wishlists")/col("total_views")),2)) \
.withColumn("contact_rate", round((col("total_contact_agent")/col("total_views")),2)) \
.orderBy("state", "total_views", ascending=[True, False])
window_spec_state = Window.partitionBy("state").orderBy(col("total_views").desc())
df_state_kpi = df_state_kpi.withColumn("row_num", row_number().over(window_spec_state)).filter(col("row_num") <= 5).withColumnRenamed("row_num", "rank_city")
window_spec = Window.partitionBy("state", "cityname").orderBy(col("total_views").desc())
agg_platform_views = df_full.groupBy("state", "cityname", "source").agg(
    count("*").alias("total_views")
).orderBy("total_views", ascending=[False])
most_platform_views_per_city = agg_platform_views.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1)
df_state_kpi = df_state_kpi.join(most_platform_views_per_city.select("state","cityname","source"), on=["state","cityname"], how="left")

# PLATFORM KPI
df_platform_kpi = df_full.groupBy("source").agg(
    count("*").alias("total_views"),
    count(when(col("is_wishlisted") == True, 1)).alias("total_wishlists"),
    count(when(col("call_to_action") == "contact_agent", 1)).alias("total_contact_agent"),
    min(col("price")).alias("min_price"),
    max(col("price")).alias("max_price"),
    count(when(col("is_active") == True, 1)).alias("total_active_apartment"),
    count(when(col("is_active") == False, 1)).alias("total_non-active_apartment"),
    round(avg(col("price")),2).alias("avg_price"),
).withColumn("wishlist_rate", round((col("total_wishlists")/col("total_views")),2)) \
.withColumn("contact_rate", round((col("total_contact_agent")/col("total_views")),2))
agg_views = df_full.groupBy("source", "apartment_id").agg(
    count("*").alias("total_views")
).orderBy("total_views", ascending=[False])

window_spec = Window.partitionBy(["source"]).orderBy(col("total_views").desc())
ranked_views = agg_views.withColumn("row_num", row_number().over(window_spec))
top3_viewed_apartment_source = ranked_views.filter(col("row_num") <= 3).drop("row_num")
top3_viewed_apartment_source = top3_viewed_apartment_source.groupBy("source").agg(concat_ws(", ", collect_list("apartment_id")).alias("top_apartments"))
df_platform_kpi = df_platform_kpi.join(top3_viewed_apartment_source.select("source", "top_apartments"), on='source', how="left")
df_full.unpersist()

print("Cleaned Transform Aggregation Success")

df_full.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/rentapart_fulldetailed_cleaned.parquet')
df_apartment_perf.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/kpi_apartment_performance.parquet')
df_hour_kpi.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/kpi_hour_summary.parquet')
df_dom_kpi.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/kpi_dayofmonth_summary.parquet')
df_state_kpi.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/kpi_state.parquet')
df_platform_kpi.write.mode('overwrite').parquet(f'gs://{bucket_name}/{database}/silver_sample/kpi_platform.parquet')

print("Load Success")

spark.stop()
