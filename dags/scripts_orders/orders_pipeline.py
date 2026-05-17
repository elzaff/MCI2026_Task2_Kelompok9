from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="orders_ecommerce_pipeline",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    description="Micro-batching Orders API -> Spark -> ClickHouse",

) as dag:
    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts_orders/fetch_orders.py",
    )

    process_orders = BashOperator(
        task_id="process_orders_spark",
        bash_command="python /opt/airflow/dags/scripts_orders/process_orders.py",
    )

    fetch_orders >> process_orders
