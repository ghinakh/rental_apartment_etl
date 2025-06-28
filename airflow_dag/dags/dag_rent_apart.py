from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
import os

PROJECT_ID = os.getenv('PROJECT_ID')
REGION = os.getenv('REGION')
CLUSTER_NAME = os.getenv('CLUSTER_NAME')
BUCKET_NAME = os.getenv('GCP_BUCKET')
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
    },
    "software_config": {"image_version": "2.0-debian10"},
    "gce_cluster_config": {
        "service_account": f"astro-airflow-gcs@{PROJECT_ID}.iam.gserviceaccount.com"
    }
}

with DAG(
    dag_id="dev-dag-dataproc",
    schedule="0 0 * * *",  # Jam 07:00 WIB setiap hari
    start_date=datetime(2025, 6, 27),
    catchup=False,
    tags=["portofolio"]
) as dag:
    
    # Task1: Extract MySQL to GCS
    extract_task = BashOperator(
        task_id='extract_mysql_to_gcs',
        bash_command='python /usr/local/airflow/include/mysql_to_gcs.py'
    )

    # Task2: Create Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # Task3: COPY file job to bucket
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_file_job_to_gcs', 
        src='/usr/local/airflow/include/pyspark_job.py', 
        dst='rental_apartment_app/code/pyspark_job.py', 
        bucket=BUCKET_NAME 
    )

    # Task4: Submit PySpark Job
    PYSPARK_URI = f'gs://{BUCKET_NAME}/rental_apartment_app/code/pyspark_job.py'
    PYSARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "args": [
                "--bucket_name", BUCKET_NAME,
                "--date_logic", "2025-06-27"  # ubah di sini kalau mau ganti
            ]
        }
    }

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=PYSARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Task5: Delete Dataproc Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task6: Load data from GCS to BigQuery
    load_task = BashOperator(
        task_id='gcs_to_bq', 
        bash_command='python /usr/local/airflow/include/gcs_to_bq.py' 
    ) 

    extract_task >> create_cluster >> upload_to_gcs >> submit_job >> delete_cluster >> load_task