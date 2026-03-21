"""
PHASE ETL : TRANSFORM SQL
Lecture Bronze Parquet — Nettoyage/Enrichissement PySpark — Écriture Silver (MinIO)
Refacto OOP : utilise ConfigManager, Logger, SparkManager centralisés, pipeline clair et testable.
"""

from pathlib import Path
from typing import Optional

from core.base import Transformer
from core.config import ConfigManager
from core.logger import get_logger, setup_logging
from core.spark_manager import SparkManager

logger = get_logger(__name__)


class BooksSQLTransformer(Transformer):
    """
    Transforme, nettoie et enrichit les données SQL (bronze-layer) puis écrit le silver-layer (MinIO & local)
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        super().__init__(name="BooksSQLTransformer")
        self.config = config or ConfigManager.get_instance()
        # Chemins clean avec config
        self.bronze_sql = Path(self.config.paths.bronze_layer) / "sql"
        self.silver_local = Path(self.config.paths.silver_layer) / "books_enriched"
        self.silver_s3 = "s3a://silver-layer/sql/books_enriched"
        # Spark Manager core
        self.spark_manager = SparkManager()
    

    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            logger.info("Lecture Bronze parquet")
            authors = spark.read.parquet(str(self.bronze_sql / "authors"))
            categories = spark.read.parquet(str(self.bronze_sql / "categories"))
            books = spark.read.parquet(str(self.bronze_sql / "books"))

            # CLEAN
            authors_clean = self.clean_authors(authors)
            categories_clean = self.clean_categories(categories)
            books_clean = self.clean_books(books)

            # ENRICH
            books_enriched = self.enrich_books(
                books_clean, authors_clean, categories_clean
            )
            logger.info("Preview enriched dataset :")
            books_enriched.show(5, truncate=False)

            # WRITE
            self.write_silver(books_enriched)
            spark.stop()
            logger.info("✓ Transform terminé")
            return True
        except Exception as e:
            logger.error(f"Transform SQL failed: {e}")
            return False

    def clean_authors(self, df):
        from pyspark.sql.functions import col, trim, upper
        from pyspark.sql.types import IntegerType
        return df \
            .withColumn("author_id", col("author_id").cast(IntegerType())) \
            .withColumn("name", trim(col("name"))) \
            .withColumn("country", upper(trim(col("country")))) \
            .dropDuplicates(["author_id"]) \
            .filter(col("name").isNotNull())

    def clean_categories(self, df):
        from pyspark.sql.functions import col, trim
        from pyspark.sql.types import IntegerType
        return df \
            .withColumn("category_id", col("category_id").cast(IntegerType())) \
            .withColumn("category_name", trim(col("category_name"))) \
            .dropDuplicates(["category_id"]) \
            .filter(col("category_name").isNotNull())

    def clean_books(self, df):
        from pyspark.sql.functions import col, trim
        from pyspark.sql.types import IntegerType, DoubleType
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

    def enrich_books(self, books, authors, categories):
        from pyspark.sql.functions import col, current_timestamp, when
        df = books \
            .join(authors, "author_id", "left") \
            .join(categories, "category_id", "left")
        df = df.select(
            "book_id", "title", "price", "rating", "stock",
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
        df = df.withColumn("transform_timestamp", current_timestamp())
        return df

    def write_silver(self, df):
        logger.info("Writing Silver dataset to MinIO")
        # MinIO (S3A)
        df.repartition(1).write.mode("overwrite").parquet(self.silver_s3)
        logger.info(f"✓ Data saved to {self.silver_s3}")
        # Local backup aussi
        self.silver_local.mkdir(parents=True, exist_ok=True)
        df.repartition(1).write.mode("overwrite").parquet(str(self.silver_local))
        logger.info(f"✓ Local backup saved to {self.silver_local}")



if __name__ == "__main__":
    setup_logging()
    transformer = BooksSQLTransformer()
    success = transformer.execute()
    import sys
    sys.exit(0 if success else 1)