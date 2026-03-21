# orchestration/dags/raw_ingestion_daily.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Default arguments for all tasks ──────────────────────
# These apply to every task in the DAG
default_args = {
    "owner"           : "data-engineering",
    "depends_on_past" : False,        # Don't wait for yesterday's run
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries"         : 2,            # Retry twice on failure
    "retry_delay"     : timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────
dag = DAG(
    dag_id="raw_ingestion_daily",
    default_args=default_args,
    description="Daily ingestion of NYC taxi data to Raw layer",
    schedule_interval="0 0 * * *",   # Every day at midnight (cron)
    catchup=False,                    # Don't backfill missed runs
    tags=["raw", "ingestion", "nyc-taxi"],
)

# ── Task 1: Health Check ──────────────────────────────────
# Always check MinIO is available before starting
def check_minio_health():
    import boto3
    from botocore.client import Config
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print("MinIO healthy. Buckets: " + str(buckets))
    if "lakehouse" not in buckets:
        raise Exception("lakehouse bucket not found!")
    return "MinIO health check passed"

health_check = PythonOperator(
    task_id="check_minio_health",
    python_callable=check_minio_health,
    dag=dag,
)

# ── Task 2: Download Data ─────────────────────────────────
download_data = BashOperator(
    task_id="download_taxi_data",
    bash_command="cd /opt/airflow && python scripts/download_data.py",
    dag=dag,
)

# ── Task 3: Upload to Raw ─────────────────────────────────
upload_to_raw = BashOperator(
    task_id="upload_to_raw_layer",
    bash_command="cd /opt/airflow && python ingestion/batch/upload_to_raw.py",
    dag=dag,
)

# ── Task 4: Validate Upload ───────────────────────────────
def validate_raw_upload():
    import boto3
    from botocore.client import Config
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )
    objects = s3.list_objects_v2(
        Bucket="lakehouse",
        Prefix="raw/year=2023"
    ).get("Contents", [])
    if len(objects) == 0:
        raise Exception("No files found in raw layer!")
    total_mb = sum(o["Size"] for o in objects) / (1024*1024)
    print("Raw layer validated: " + str(len(objects)) + " files, " + str(round(total_mb, 1)) + " MB")
    return "validation passed"

validate_raw = PythonOperator(
    task_id="validate_raw_upload",
    python_callable=validate_raw_upload,
    dag=dag,
)

# ── Task Dependencies ─────────────────────────────────────
# This defines the ORDER tasks run in
# health_check → download → upload → validate
health_check >> download_data >> upload_to_raw >> validate_raw