"""
PHASE ETL : TRANSFORM

Objectifs :
- Lire les données depuis MinIO (CSV)
- Nettoyer et typer les données
- Appliquer des règles de qualité
- Écrire les données VALIDES en zone silver-layer
- Logger les anomalies sans casser le pipeline
"""

import os
import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, when,substring
from dotenv import load_dotenv
from pyspark.sql.types import IntegerType



load_dotenv(override=False)

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9001")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_S3_client():
    """
      creer un S3 CLIENT
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY, 
    )



def ensure_bucket_exists(bucket_name: str):
    """
    Vérifie l'existence d'un bucket MinIO, le crée si nécessaire
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' already exists ")
    except ClientError:
        logging.info(f"Bucket '{bucket_name}' not found → creating ")
        s3_client.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created successfully")



def s3_folder_exists(s3_client, bucket, prefix):
    """
    Vérifie si un dossier (prefix) contient déjà des objets
    """
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return "Contents" in resp and len(resp["Contents"]) > 0


def create_spark_session() -> SparkSession:
    """
    Crée une SparkSession configurée pour MinIO (S3A)
    """
    logging.info("Initializing Spark session")

    jar_path = "/opt/airflow/hadoop-aws-3.2.4.jar,/opt/airflow/aws-java-sdk-bundle-1.11.1026.jar"

    return (
        SparkSession.builder
        .appName("TransformBooks")
        .config("spark.jars", jar_path)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_raw_books(spark: SparkSession):
    path = "s3a://bronze-layer/books/books_*.csv"
    logging.info(f"Reading RAW data from {path}")

    return (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


def clean_books(df_raw):
    logging.info("Cleaning books data")

    current_year = 2026

    # 1️⃣ cast propre
    df = df_raw.withColumn(
        "Year_Int",
        col("Year-Of-Publication").cast(IntegerType())
    )

    # 2️⃣ filtre années aberrantes
    df = df.withColumn(
        "Year-Of-Publication",
        when(
            (col("Year_Int") < 1000) | (col("Year_Int") > current_year),
            None
        ).otherwise(col("Year_Int").cast(IntegerType()))
    ).drop("Year_Int")

    # 3️⃣ décennie en entier garanti
    df = df.withColumn(
        "Decade",
        (col("Year-Of-Publication") / 10).cast(IntegerType()) * 10
    )

    # 4️⃣ initial auteur
    df = df.withColumn(
        "Author_Initial",
        substring(col("Book-Author"), 1, 1)
    )

    return df

def write_silver_layer_books(df_valid):
    """
    Écriture des données VALIDES en zone silver-layer
    (MinIO + local)
    """
    s3_output = "s3a://silver-layer/csv-books/"
    local_output = Path("/opt/airflow/data/silver-layer/csv-books")

    ensure_bucket_exists("silver-layer")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    # --- MinIO ---
    if not s3_folder_exists(s3_client, "silver-layer", "csv-books/"):
        logging.info(f"Writing silver-layer data to {s3_output}")
        df_valid.coalesce(1).write.mode("overwrite").parquet(s3_output)
    else:
        logging.info("[SKIP] silver-layer data already exists in MinIO")

    # --- Local ---
    if not os.path.exists(local_output) or not os.listdir(local_output):
        logging.info(f"Writing silver-layer data locally to {local_output}")
        os.makedirs(local_output, exist_ok=True)
        df_valid.coalesce(1).write.mode("overwrite").parquet(str(local_output)) # don't accept path object because to convert str due to parquet 
    else:
        logging.info("[SKIP] silver-layer data already exists locally")

if __name__ == "__main__":

    spark = create_spark_session()

    # 1️READ
    df_raw = read_raw_books(spark)
    df_raw.show(5, truncate=False)

    # 2️CLEAN
    df_clean = clean_books(df_raw)
    df_clean.show(5, truncate=False)



    # 4️ WRITE
    write_silver_layer_books(df_clean)

    # 5️ SCHEMA
    df_clean.printSchema()

    spark.stop()
