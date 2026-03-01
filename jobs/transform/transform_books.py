"""
PHASE ETL : TRANSFORM

Objectifs :
- Lire les données RAW depuis MinIO (JSON)
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
from pyspark.sql.functions import regexp_replace, col, when, to_timestamp
from dotenv import load_dotenv

# ============================================================
# ENV & LOGGING
# ============================================================

load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9001")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# MINIO / S3 HELPERS
# ============================================================

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

# ============================================================
# SPARK
# ============================================================

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

# ============================================================
# ETL – TRANSFORM
# ============================================================

def read_raw_books(spark: SparkSession):
    """
    Lecture des fichiers bronze-layer depuis MinIO
    """
    path = "s3a://bronze-layer/books/books_*.json"
    logging.info(f"Reading RAW data from {path}")

    return (
        spark.read
        .option("multiline", "true")
        .json(path)
    )


def clean_books(df_raw):
    """
    Nettoyage et typage des données
    """
    logging.info("Cleaning books data")

    df = df_raw.withColumn(
        "price",
        regexp_replace(col("price"), "[^0-9.]", "").cast("double")
    )

    df = df.withColumn(
        "rating",
        when(col("rating") == "One", 1)
        .when(col("rating") == "Two", 2)
        .when(col("rating") == "Three", 3)
        .when(col("rating") == "Four", 4)
        .when(col("rating") == "Five", 5)
        .otherwise(None)
    )

    df = df.withColumn(
        "scraped_at",
        to_timestamp(col("scraped_at"))
    )

    return df


def apply_data_quality_checks(df):
    """
    Applique les règles de qualité :
    - title non null
    - price > 0
    - rating entre 1 et 5
    - scraped_at non null
    """
    logging.info("Applying data quality rules")

    valid_df = df.filter(
        (col("title").isNotNull()) &
        (col("price").isNotNull()) &
        (col("price") > 0) &
        (col("rating").between(1, 5)) &
        (col("scraped_at").isNotNull())
    )

    invalid_df = df.subtract(valid_df)

    logging.info(f"Valid records  : {valid_df.count()}")
    logging.info(f"Invalid records: {invalid_df.count()}")

    return valid_df, invalid_df


def write_silver_layer_books(df_valid):
    """
    Écriture des données VALIDES en zone silver-layer
    (MinIO + local)
    """
    s3_output = "s3a://silver-layer/books/"
    local_output = Path("/opt/airflow/data/silver-layer/books")

    ensure_bucket_exists("silver-layer")

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    # --- MinIO ---
    if not s3_folder_exists(s3_client, "silver-layer", "books/"):
        logging.info(f"Writing silver-layer data to {s3_output}")
        df_valid.write.mode("overwrite").parquet(s3_output)
    else:
        logging.info("[SKIP] silver-layer data already exists in MinIO")

    # --- Local ---
    if not os.path.exists(local_output) or not os.listdir(local_output):
        logging.info(f"Writing silver-layer data locally to {local_output}")
        os.makedirs(local_output, exist_ok=True)
        df_valid.write.mode("overwrite").parquet(str(local_output)) # don't accept path object because to convert str due to parquet 
    else:
        logging.info("[SKIP] silver-layer data already exists locally")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = create_spark_session()

    # 1️READ
    df_raw = read_raw_books(spark)
    df_raw.show(5, truncate=False)

    # 2️CLEAN
    df_clean = clean_books(df_raw)
    df_clean.show(5, truncate=False)

    # 3️ QUALITY
    df_valid, df_invalid = apply_data_quality_checks(df_clean)

    if df_invalid.count() > 0:
        logging.warning("Some invalid records were detected")
        df_invalid.show(5, truncate=False)

    # 4️ WRITE
    write_silver_layer_books(df_valid)

    # 5️ SCHEMA
    df_valid.printSchema()

    spark.stop()
