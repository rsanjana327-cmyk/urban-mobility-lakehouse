# orchestration/dags/bronze_processing_daily.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner"           : "data-engineering",
    "depends_on_past" : False,
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=10),
}

dag = DAG(
    dag_id="bronze_processing_daily",
    default_args=default_args,
    description="Daily Bronze layer processing with Delta Lake",
    schedule_interval="30 0 * * *",  # 12:30am — after raw ingestion
    catchup=False,
    tags=["bronze", "delta-lake", "nyc-taxi"],
)

# ── Task 1: Wait for Raw Ingestion ────────────────────────
# ExternalTaskSensor waits for another DAG to complete
# This is how you chain DAGs together in production
wait_for_raw = ExternalTaskSensor(
    task_id="wait_for_raw_ingestion",
    external_dag_id="raw_ingestion_daily",
    external_task_id="validate_raw_upload",
    timeout=3600,           # Wait up to 1 hour
    poke_interval=60,       # Check every 60 seconds
    dag=dag,
)

# ── Task 2: Run Bronze Ingestion ──────────────────────────
run_bronze = BashOperator(
    task_id="run_bronze_ingestion",
    bash_command="cd /opt/airflow && python transformation/bronze/batch_bronze.py",
    dag=dag,
)

# ── Task 3: Validate Bronze ───────────────────────────────
def validate_bronze():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("BronzeValidation").getOrCreate()
    df = spark.read.format("delta").load("s3a://lakehouse/bronze/yellow_taxi")
    count = df.count()
    if count == 0:
        raise Exception("Bronze table is empty!")
    print("Bronze validated: " + str(count) + " records")
    spark.stop()

validate_bronze_task = PythonOperator(
    task_id="validate_bronze_layer",
    python_callable=validate_bronze,
    dag=dag,
)

# ── Task Dependencies ─────────────────────────────────────
wait_for_raw >> run_bronze >> validate_bronze_task