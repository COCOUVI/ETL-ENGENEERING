"""
PHASE ETL : TRANSFORM
Lecture Bronze PARQUET
Nettoyage + Enrichissement avec PySpark
Ecriture Silver PARQUET dans MinIO
"""

import logging
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    when
)
from pyspark.sql.types import IntegerType, DoubleType

# ============================================================
# CONFIGURATION
# ============================================================

BRONZE_PATH = "/opt/airflow/data/bronze-layer/sql"

SILVER_S3_PATH = "s3a://silver-layer/sql/books_enriched"
SILVER_LOCAL_PATH = "/opt/airflow/data/silver-layer/books_enriched"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = SparkSession.builder \
        .appName("ETL-Transform-Books") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ) \
        .config(
            "spark.jars",
            "/opt/airflow/hadoop-aws-3.2.4.jar,"
            "/opt/airflow/aws-java-sdk-bundle-1.11.1026.jar"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logging.info("✓ Spark session created")

    return spark


# ============================================================
# READ BRONZE PARQUET
# ============================================================

def read_bronze_data(spark):

    logging.info("Reading Bronze parquet datasets")

    authors = spark.read.parquet(f"{BRONZE_PATH}/authors/")
    categories = spark.read.parquet(f"{BRONZE_PATH}/categories/")
    books = spark.read.parquet(f"{BRONZE_PATH}/books/")

    logging.info(f"Authors rows: {authors.count()}")
    logging.info(f"Categories rows: {categories.count()}")
    logging.info(f"Books rows: {books.count()}")

    return authors, categories, books


# ============================================================
# CLEANING
# ============================================================

def clean_authors(df):

    logging.info("Cleaning authors")

    return df \
        .withColumn("author_id", col("author_id").cast(IntegerType())) \
        .withColumn("name", trim(col("name"))) \
        .withColumn("country", upper(trim(col("country")))) \
        .dropDuplicates(["author_id"]) \
        .filter(col("name").isNotNull())


def clean_categories(df):

    logging.info("Cleaning categories")

    return df \
        .withColumn("category_id", col("category_id").cast(IntegerType())) \
        .withColumn("category_name", trim(col("category_name"))) \
        .dropDuplicates(["category_id"]) \
        .filter(col("category_name").isNotNull())


def clean_books(df):

    logging.info("Cleaning books")

    return df \
        .withColumn("book_id", col("book_id").cast(IntegerType())) \
        .withColumn("author_id", col("author_id").cast(IntegerType())) \
        .withColumn("category_id", col("category_id").cast(IntegerType())) \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("rating", col("rating").cast(IntegerType())) \
        .withColumn("stock", col("stock").cast(IntegerType())) \
        .withColumn("title", trim(col("title"))) \
        .filter(col("title").isNotNull()) \
        .filter(col("price") > 0) \
        .filter(col("rating").between(1, 5)) \
        .dropDuplicates(["book_id"])


# ============================================================
# ENRICHMENT
# ============================================================

def enrich_books(books, authors, categories):

    logging.info("Joining datasets")

    df = books \
        .join(authors, "author_id", "left") \
        .join(categories, "category_id", "left")

    df = df.select(
        "book_id",
        "title",
        "price",
        "rating",
        "stock",
        col("name").alias("author_name"),
        col("country").alias("author_country"),
        "category_name",
        "created_at"
    )

    df = df.withColumn(
        "data_quality",
        when(
            col("author_name").isNotNull() &
            col("category_name").isNotNull(),
            "COMPLETE"
        ).otherwise("INCOMPLETE")
    )

    df = df.withColumn(
        "transform_timestamp",
        current_timestamp()
    )

    return df


# ============================================================
# WRITE SILVER
# ============================================================

def write_silver(df):

    logging.info("Writing Silver dataset to MinIO")

    df.repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(SILVER_S3_PATH)

    logging.info("✓ Data saved to MinIO")

    Path(SILVER_LOCAL_PATH).mkdir(parents=True, exist_ok=True)

    df.repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(SILVER_LOCAL_PATH)

    logging.info("✓ Local backup saved")


# ============================================================
# PIPELINE
# ============================================================

def transform():

    spark = create_spark_session()

    authors, categories, books = read_bronze_data(spark)

    authors_clean = clean_authors(authors)
    categories_clean = clean_categories(categories)
    books_clean = clean_books(books)

    books_enriched = enrich_books(
        books_clean,
        authors_clean,
        categories_clean
    )

    logging.info("Preview of enriched dataset")
    books_enriched.show(5, truncate=False)

    write_silver(books_enriched)

    spark.stop()

    logging.info("✓ Transform completed successfully")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    transform()