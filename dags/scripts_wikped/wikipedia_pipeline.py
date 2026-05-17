from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'wikipedia_realtime_stream',
    default_args=default_args,
    schedule_interval='*/10 * * * *', # Berjalan setiap 2 menit
    catchup=False,
    max_active_runs=1,
    description='Micro-batching Wikipedia API -> Spark -> ClickHouse'
) as dag:

    ingest_stream = BashOperator(
        task_id='fetch_recent_changes',
        bash_command='python /opt/airflow/dags/scripts/fetch_wikipedia_stream.py'
    )

    process_analytics = BashOperator(
        task_id='process_heavy_hitters_spark',
        bash_command='python /opt/airflow/dags/scripts/process_wikipedia_spark.py'
    )

    ingest_stream >> process_analytics