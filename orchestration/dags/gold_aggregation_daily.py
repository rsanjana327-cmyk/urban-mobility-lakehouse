# orchestration/dags/gold_aggregation_daily.py
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
    dag_id="gold_aggregation_daily",
    default_args=default_args,
    description="Daily Gold aggregation for business analytics",
    schedule_interval="0 4 * * *",   # 4am — after silver
    catchup=False,
    tags=["gold", "aggregation", "business"],
)

wait_for_silver = ExternalTaskSensor(
    task_id="wait_for_silver",
    external_dag_id="silver_transform_daily",
    external_task_id="run_silver_transform",
    timeout=3600,
    poke_interval=60,
    dag=dag,
)

run_gold = BashOperator(
    task_id="run_gold_aggregation",
    bash_command="cd /opt/airflow && python transformation/gold/batch_gold.py",
    dag=dag,
)

wait_for_silver >> run_gold