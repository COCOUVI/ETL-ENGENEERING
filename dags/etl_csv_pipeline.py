from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_csv_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "books", "spark", "minio"],
) as dag:

    extract_csv_books = BashOperator(
        task_id="extract_books_csv",
        bash_command="python /opt/airflow/jobs/extract/extract_csv.py",
    )

    load_csv_to_minio = BashOperator(
        task_id="load_csv_to_minio",
        bash_command="python /opt/airflow/jobs/load/load_csv_to_minio.py",
    )

    transform_books_csv = BashOperator(
        task_id="transform_books_csv",
        bash_command="python /opt/airflow/jobs/transform/transform_csv_books.py",
    )

    extract_csv_books >> load_csv_to_minio >> transform_books_csv
