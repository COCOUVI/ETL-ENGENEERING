"""
PHASE ETL : TRANSFORM CSV BOOKS
- Lire les données depuis MinIO (CSV)
- Nettoyage, typage, règles qualité PySpark
- Écriture en silver-layer + logging anomalies
- Refacto OOP, outils core uniquement (config, Spark, MinIO, logger)
"""

from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, substring
from pyspark.sql.types import IntegerType
from core.base import Transformer
from core.config import ConfigManager
from core.logger import get_logger, setup_logging
from core.spark_manager import SparkManager
from core.minio_client import MinIOClient

logger = get_logger(__name__)


class BooksCSVTransformer(Transformer):
    """
    Pipeline de transformation des books CSV (bronze-layer/csv) → silver-layer (MinIO + local)
    Nettoyage, typage, enrichissement, logging anomalies et sauvegarde.
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        super().__init__(name="BooksCSVTransformer")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        self.spark_manager = SparkManager(self.config)
        self.silver_bucket = "silver-layer"
        self.silver_prefix = "csv-books/"
        self.silver_local = Path(self.config.paths.silver_layer) / "csv-books"
        # CSV sur MinIO (bronze)
        self.bronze_books_path = f"s3a://{self.config.minio.bronze_bucket}/books/books_*.csv"
        self.current_year = self.config.csv_transform_year if hasattr(self.config, "csv_transform_year") else 2026

    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            # 1. Read
            df_raw = self.read_raw_books(spark)
            self.log_info("Aperçu RAW")
            df_raw.show(5, truncate=False)

            # 2. Clean
            df_clean = self.clean_books(df_raw)
            self.log_info("Aperçu clean")
            df_clean.show(5, truncate=False)

            # 3. Write Silver (valid only, on peut split si rules)
            self.write_silver(df_clean)

            df_clean.printSchema()
            spark.stop()
            self.log_info("✓ BooksCSVTransformer terminé avec succès")
            return True
        except Exception as e:
            self.log_error(f"BooksCSVTransformer FAILED: {e}")
            return False

    def read_raw_books(self, spark) -> DataFrame:
        self.log_info(f"Lecture CSV depuis {self.bronze_books_path}")
        return (
            spark.read
            .option("header", "true")
            .option("sep", ";")
            .option("mode", "PERMISSIVE")
            .csv(self.bronze_books_path)
        )

    def clean_books(self, df_raw: DataFrame) -> DataFrame:
        self.log_info("Cleaning books data")
        df = df_raw.withColumn(
            "Year_Int",
            col("Year-Of-Publication").cast(IntegerType())
        )
        df = df.withColumn(
            "Year-Of-Publication",
            when(
                (col("Year_Int") < 1000) | (col("Year_Int") > self.current_year),
                None
            ).otherwise(col("Year_Int").cast(IntegerType()))
        ).drop("Year_Int")
        df = df.withColumn(
            "Decade",
            (col("Year-Of-Publication") / 10).cast(IntegerType()) * 10
        )
        df = df.withColumn(
            "Author_Initial",
            substring(col("Book-Author"), 1, 1)
        )
        return df

    def write_silver(self, df_clean: DataFrame):
        """
        Écrit CSV transformé dans MinIO + local, évite overwrite si déjà existant.
        """
        self.minio.ensure_bucket_exists(self.silver_bucket)
        minio_path = f"s3a://{self.silver_bucket}/{self.silver_prefix}"

        # MinIO
        s3_exists = self.minio.folder_exists(self.silver_prefix, self.silver_bucket)
        if not s3_exists:
            self.log_info(f"Writing silver data to {minio_path}")
            df_clean.coalesce(1).write.mode("overwrite").parquet(minio_path)
        else:
            self.log_info("[SKIP] silver-layer data already exists in MinIO")

        # Local
        if not self.silver_local.exists() or not any(self.silver_local.iterdir()):
            self.log_info(f"Writing silver data locally to {self.silver_local}")
            self.silver_local.mkdir(parents=True, exist_ok=True)
            df_clean.coalesce(1).write.mode("overwrite").parquet(str(self.silver_local))
        else:
            self.log_info("[SKIP] silver-layer data already exists locally")

if __name__ == "__main__":
    setup_logging()
    transformer = BooksCSVTransformer()
    success = transformer.execute()
    import sys
    sys.exit(0 if success else 1)