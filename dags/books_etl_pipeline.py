from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="books_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "books", "spark", "minio"],
) as dag:

    scrape_books = BashOperator(
        task_id="scrape_books",
        bash_command="python /opt/airflow/jobs/extract/scrape_books.py",
    )

    load_raw_to_minio = BashOperator(
        task_id="load_raw_to_minio",
        bash_command="python /opt/airflow/jobs/load/load_to_minio.py",
    )

    transform_books = SparkSubmitOperator(
        task_id="transform_books",
        application="/opt/spark/jobs/transform/transform_books.py",
        conn_id="spark_default",
        verbose=True,
    )

    scrape_books >> load_raw_to_minio >> transform_books
