# **📝 Project Overview**

The Rental Apartment ETL Pipeline is designed to process, clean, and enrich rental apartment data from multiple sources to build a comprehensive analytical dataset for performance monitoring and decision-making. The pipeline integrates real-time streaming data from Confluent Cloud Kafka and transactional data from MySQL, simulating an apartment rental app. Data ingestion, transformation, and loading processes are fully orchestrated using Apache Airflow (via Astronomer). 

Key technologies include Google Cloud Storage (GCS) for storing both raw and processed data, Dataproc with PySpark for scalable data processing, and Google BigQuery for analytical querying and visualization. The pipeline was initially developed and tested in a Docker container environment, then deployed to the cloud to simulate a production setup. The pipeline also incorporates scheduled automation, role-based access control via Google Cloud IAM, and dedicated service accounts to ensure security and operational efficiency.

# 🎯 Objectives

- Build an end-to-end, automated ETL pipeline to process apartment rental data from streaming and OLTP sources on a scheduled basis.
- Combine real-time user viewings ingested from Kafka with apartment reference data stored in MySQL to create unified, enriched datasets.
- Utilize Dataproc with PySpark to enable distributed, efficient processing for large datasets and optimize processing time compared to local execution.
- Enforce security best practices by using separate service accounts with minimal required permissions for each component in the pipeline.
- Validate, clean, and transform incoming data to ensure data quality for analytical processing.
- Produce key performance indicators (KPIs) such as apartment performance, hour summary, day of month summary, platform level, and state level.
- Load fact and dimension tables into BigQuery to support interactive querying, KPI tracking, and reporting.
- Showcase the ability to manage and schedule complex workflows using Apache Airflow, ensuring daily, reliable data processing.

# 🗃️ Data Source & Scenario

### Source Dataset:

- Original dataset from GitHub: [Mr. Sid's Dataset](https://github.com/sidoncloud/udemy-aws-de-labs/tree/main/Lab3-%20ETL%20Glue%20Python%20Shell%7C%20Step%20functions-S3-Redshift%20%7C%20Rental%20Apartments/data)
    - user_viewings = 4999
    - apartments = 10000
    - apartment_attribute = 10000

### Simulated Data Flow:

- **Apartments & Apartment Attribute:**
    - Stored in **MySQL** to simulate an OLTP database.
- **User Viewings:**
    - Produced via **Confluent Cloud Kafka** to simulate real-time streaming ingestion.
    - Kafka Sink Connector automatically delivers stream data to GCS in JSON format.
    - Generate dummy data based on the original dataset to support ETL pipeline testing with a larger data volume than the source dataset.
        - user_viewings = 15001

### Final Workflow:

- Data stored in Google Cloud Storage (GCS) in JSON format is processed on a scheduled basis using Apache Airflow (Astronomer).
- The data is enriched using information from MySQL Apartment and Apartment Attributes tables.

# 🛠️ Tech Stack

- **Apache Airflow (ETL Orchestration via Astronomer)**
- **Docker (Containerization)**
- **Google Cloud Storage (GCS)**
- **Google BigQuery**
- **MySQL**
- **Confluent Cloud Kafka**
- **GCS Sink Connector**
- **Python (Pandas, Google Cloud SDK, etc)**
- **Dataproc Cluster**
- **Pyspark (Docker)**
- **Jupyter Notebook**
- **Google Colaboration**
- **Google Cloud IAM (Service Accounts)**

# 📊 Key Metrics Generated (per Day)
🏡 KPI Apartment Performance
⏳ KPI Hour Summary
🕛 KPI Day of month Summary
🌆 KPI State
🗂️ KPI Platform

# ⏰ Schedule

- The pipeline is scheduled to run daily at 00:00 UTC / 07:00 WIB.

# 💡Key Point

- The pyspark_local.py and pyspark_job.py files:
    - pyspark_local.py contains heavy transformation and aggregation processes that are run via docker
    - pyspark_job.py is the same as pyspark_local.py, but run in the cloud (distributed and parallel)
- Execution time (with the same data → n user_viewings = 4999):
    - `pyspark_local.py` started at 22:35:51 UTC and finished at 23:15:27 UTC, processing for 39 minutes, 36 seconds
    - `pyspark_job.py` started at 07:14:14 WIB and finished at 07:16:09 WIB, taking approximately 2 minutes to complete. The entire DAG process — including extracting from MySQL, creating the cluster, running the job, deleting the cluster, and storing the data into BigQuery — was completed in under 15 minutes, demonstrating better time efficiency.

# 🔍 Additional Notes

- The pipeline is executed using local Airflow with Docker via Astronomer CLI.
- Airflow connection was made for using Dataproc in GCP:
    - conn_id: google_cloud_default
    - conn_type: google_cloud_platform
    - json:
      {
        ”extra__google_cloud_platform__key_path”: “/usr/local/airflow/include/service_account.json”,
        ”extra__google_cloud_platform__project”: “project-id”,
        ”extra__google_cloud_platform__scope”: “https://www.googleapis.com/auth/cloud-platform”
      }
- Google Cloud environments are configured using separate Service Accounts with minimal required roles:
    - Kafka Sink Connector to GCS:
        - Project Level: Storage Object Admin, Storage Object Creator, Storage Object Viewer
        - Resource Level (GCS): legacyBucketReader
    - Airflow to Dataproc, BigQuery, and GCS: BigQuery Data Editor, BigQuery Data Viewer, BigQuery Job User, Dataproc Editor, Dataproc Worker, Service Account User, Storage Object User, Viewer
