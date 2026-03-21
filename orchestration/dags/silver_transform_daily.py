# orchestration/dags/silver_transform_daily.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    dag_id="silver_transform_daily",
    default_args=default_args,
    description="Daily Silver transformation with dedup and quality checks",
    schedule_interval="0 2 * * *",   # 2am — after bronze
    catchup=False,
    tags=["silver", "quality", "nyc-taxi"],
)

wait_for_bronze = ExternalTaskSensor(
    task_id="wait_for_bronze",
    external_dag_id="bronze_processing_daily",
    external_task_id="validate_bronze_layer",
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

run_silver = BashOperator(
    task_id="run_silver_transform",
    bash_command="cd /opt/airflow && python transformation/silver/batch_silver.py",
    dag=dag,
)

wait_for_bronze >> run_silver