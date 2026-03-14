"""
PHASE ETL : TRANSFORM - Transformation avec PySpark
Nettoyage, validation et enrichissement des données du bronze-layer
"""

import logging
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, concat_ws, upper, trim, when, regexp_replace
from pyspark.sql.types import IntegerType, DoubleType
import os

# ============================================================
# CONFIGURATION
# ============================================================

# Chemins
BRONZE_SQL_PATH = "/opt/airflow/data/bronze-layer/sql"
SILVER_OUTPUT_PATH = "/opt/airflow/data/silver-layer"

# MinIO Configuration (pour écriture future)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
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
    """
    Créer la session Spark avec configuration MinIO
    """
    spark = SparkSession.builder \
        .appName("ETL-Transform-Books") \
        .config("spark.master", "local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logging.info("✓ Spark session created")
    return spark


# ============================================================
# FONCTIONS DE TRANSFORMATION
# ============================================================

def read_bronze_data(spark, bronze_path):
    """
    Lire les données du bronze-layer
    """
    logging.info(f"Reading bronze data from: {bronze_path}")
    
    # Lire les tables
    authors_df = spark.read.parquet(f"{bronze_path}/authors/*.parquet")
    categories_df = spark.read.parquet(f"{bronze_path}/categories/*.parquet")
    books_df = spark.read.parquet(f"{bronze_path}/books/*.parquet")
    
    logging.info(f"✓ Authors: {authors_df.count()} rows")
    logging.info(f"✓ Categories: {categories_df.count()} rows")
    logging.info(f"✓ Books: {books_df.count()} rows")
    
    return authors_df, categories_df, books_df


def clean_authors(df):
    """
    Nettoyer la table authors
    """
    logging.info("Cleaning authors table...")
    
    df_clean = df \
        .withColumn("name", trim(col("name"))) \
        .withColumn("country", upper(trim(col("country")))) \
        .dropDuplicates(["author_id"]) \
        .filter(col("name").isNotNull())
    
    logging.info(f"✓ Authors cleaned: {df_clean.count()} rows")
    return df_clean


def clean_categories(df):
    """
    Nettoyer la table categories
    """
    logging.info("Cleaning categories table...")
    
    df_clean = df \
        .withColumn("category_name", trim(col("category_name"))) \
        .dropDuplicates(["category_id"]) \
        .filter(col("category_name").isNotNull())
    
    logging.info(f"✓ Categories cleaned: {df_clean.count()} rows")
    return df_clean


def clean_books(df):
    """
    Nettoyer la table books
    """
    logging.info("Cleaning books table...")
    
    df_clean = df \
        .withColumn("title", trim(col("title"))) \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("rating", col("rating").cast(IntegerType())) \
        .withColumn("stock", col("stock").cast(IntegerType())) \
        .filter(col("title").isNotNull()) \
        .filter(col("price") > 0) \
        .filter(col("rating").between(1, 5)) \
        .dropDuplicates(["book_id"])
    
    logging.info(f"✓ Books cleaned: {df_clean.count()} rows")
    return df_clean


def enrich_books(books_df, authors_df, categories_df):
    """
    Enrichir les books avec jointures
    """
    logging.info("Enriching books with authors and categories...")
    
    # Jointure avec authors
    enriched_df = books_df \
        .join(authors_df, books_df.author_id == authors_df.author_id, "left") \
        .drop(authors_df.author_id)
    
    # Jointure avec categories
    enriched_df = enriched_df \
        .join(categories_df, enriched_df.category_id == categories_df.category_id, "left") \
        .drop(categories_df.category_id)
    
    # Sélectionner et réorganiser les colonnes
    enriched_df = enriched_df.select(
        col("book_id"),
        col("title"),
        col("price"),
        col("rating"),
        col("stock"),
        col("name").alias("author_name"),
        col("country").alias("author_country"),
        col("category_name"),
        col("created_at")
    )
    
    # Ajouter métadonnées de transformation
    enriched_df = enriched_df \
        .withColumn("transform_timestamp", current_timestamp()) \
        .withColumn("data_quality", 
                   when((col("author_name").isNotNull()) & (col("category_name").isNotNull()), "COMPLETE")
                   .otherwise("INCOMPLETE"))
    
    logging.info(f"✓ Books enriched: {enriched_df.count()} rows")
    
    return enriched_df


# ============================================================
# TRANSFORM
# ============================================================

def transform():
    """
    Pipeline de transformation complet
    """
    try:
        # Créer la session Spark
        spark = create_spark_session()
        
        # Lire les données bronze
        authors_df, categories_df, books_df = read_bronze_data(spark, BRONZE_SQL_PATH)
        
        # Nettoyer chaque table
        authors_clean = clean_authors(authors_df)
        categories_clean = clean_categories(categories_df)
        books_clean = clean_books(books_df)
        
        # Enrichir books
        books_enriched = enrich_books(books_clean, authors_clean, categories_clean)
        
        # Afficher un aperçu
        logging.info("\n📊 Sample of enriched data:")
        books_enriched.show(5, truncate=False)
        
        # Afficher le schéma
        logging.info("\n📋 Schema:")
        books_enriched.printSchema()
        
        # Statistiques
        logging.info("\n📈 Statistics:")
        logging.info(f"  - Total books: {books_enriched.count()}")
        avg_price = books_enriched.agg({"price": "avg"}).collect()[0][0]
        avg_rating = books_enriched.agg({"rating": "avg"}).collect()[0][0]
        total_stock = books_enriched.agg({"stock": "sum"}).collect()[0][0]
        logging.info(f"  - Average price: ${avg_price:.2f}")
        logging.info(f"  - Average rating: {avg_rating:.2f}")
        logging.info(f"  - Total stock: {total_stock}")
        
        # Créer le dossier de sortie
        Path(SILVER_OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
        
        # Sauvegarder en Parquet dans silver-layer
        output_path = f"{SILVER_OUTPUT_PATH}/books_enriched"
        books_enriched.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        logging.info(f"\n✓ Data saved to: {output_path}")
        
        # Résumé final
        logging.info("\n" + "=" * 60)
        logging.info("✓ TRANSFORM COMPLETED SUCCESSFULLY")
        logging.info("=" * 60)
        
        spark.stop()
        
    except Exception as e:
        logging.error(f"Transform failed: {e}")
        raise


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    transform()