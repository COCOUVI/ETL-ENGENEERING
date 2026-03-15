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
    dag_id="etl_sql_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "books", "spark", "minio"],
) as dag:

    extract_sql_books = BashOperator(
        task_id="extract_books_sql",
        bash_command="python /opt/airflow/jobs/extract/extract_sql.py",
    )

    load_sql_to_minio = BashOperator(
        task_id="load_sql_to_minio",
        bash_command="python /opt/airflow/jobs/load/load_sql_to_minio.py",
    )

    transform_books_sql = BashOperator(
        task_id="transform_books_sql",
        bash_command="python /opt/airflow/jobs/transform/transform_sql_books.py",
    )

    extract_sql_books >> load_sql_to_minio >> transform_books_sql
