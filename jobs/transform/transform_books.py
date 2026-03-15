"""
PHASE ETL : TRANSFORM BOOKS RAW
- Lire les données RAW depuis MinIO (JSON)
- Nettoyage, typage, règles qualité PySpark
- Écriture en silver-layer + détection anomalies, logging unifié
- Refactored OOP: utilise uniquement les outils core
"""

from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql.functions import regexp_replace, col, when, to_timestamp
from pyspark.sql import DataFrame
from core.base import Transformer
from core.config import ConfigManager
from core.logger import get_logger, setup_logging
from core.minio_client import MinIOClient
from core.spark_manager import SparkManager

logger = get_logger(__name__)


class BooksRawTransformer(Transformer):
    """
    Pipeline de transformation des books RAW (JSON bronze-layer) → silver-layer (MinIO + local)
    Applique : nettoyage, typage, vérification qualité, logging anomalies.
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        super().__init__(name="BooksRawTransformer")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        self.spark_manager = SparkManager(self.config)
        self.silver_bucket = "silver-layer"
        self.silver_prefix = "books/"
        self.silver_local = Path(self.config.paths.silver_layer) / "books"
        # Chemin des JSON RAW sur MinIO (bronze layer)
        self.bronze_books_path = f"s3a://{self.config.minio.bronze_bucket}/books/books_*.json"

    def transform(self) -> bool:
        """
        Exécute tout le pipeline : read → clean → qualité → write ; log toutes les phases.
        """
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

            # 3. Quality
            df_valid, df_invalid = self.apply_data_quality_checks(df_clean)
            if df_invalid.count() > 0:
                self.log_warning("Des lignes invalides détectées")
                df_invalid.show(5, truncate=False)

            # 4. Write
            self.write_silver(df_valid)

            # 5. Extra : show schema
            df_valid.printSchema()
            spark.stop()
            self.log_info("✓ BooksRawTransformer terminé avec succès")
            return True
        except Exception as e:
            self.log_error(f"BooksRawTransformer FAILED: {e}")
            return False

    def read_raw_books(self, spark) -> DataFrame:
        self.log_info(f"Lecture des fichiers RAW depuis {self.bronze_books_path}")
        return (
            spark.read
            .option("multiline", "true")
            .json(self.bronze_books_path)
        )

    def clean_books(self, df_raw: DataFrame) -> DataFrame:
        self.log_info("Cleaning books data")
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

    def apply_data_quality_checks(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        self.log_info("Applying data quality rules")
        valid_df = df.filter(
            (col("title").isNotNull()) &
            (col("price").isNotNull()) &
            (col("price") > 0) &
            (col("rating").between(1, 5)) &
            (col("scraped_at").isNotNull())
        )
        invalid_df = df.subtract(valid_df)
        self.log_info(f"Valid records: {valid_df.count()}")
        self.log_info(f"Invalid records: {invalid_df.count()}")
        return valid_df, invalid_df

    def write_silver(self, df_valid: DataFrame):
        """
        Écriture MinIO + local - vérifie le bucket et la présence avant d'écrire.
        """
        self.minio.ensure_bucket_exists(self.silver_bucket)
        minio_path = f"s3a://{self.silver_bucket}/{self.silver_prefix}"

        # (1) MinIO
        s3_exists = self.minio.folder_exists(self.silver_prefix, self.silver_bucket)
        if not s3_exists:
            self.log_info(f"Writing silver data to {minio_path}")
            df_valid.write.mode("overwrite").parquet(minio_path)
        else:
            self.log_info("[SKIP] silver-layer data already exists in MinIO")

        # (2) Local
        if not self.silver_local.exists() or not any(self.silver_local.iterdir()):
            self.log_info(f"Writing silver data locally to {self.silver_local}")
            self.silver_local.mkdir(parents=True, exist_ok=True)
            df_valid.write.mode("overwrite").parquet(str(self.silver_local))
        else:
            self.log_info("[SKIP] silver-layer data already exists locally")


if __name__ == "__main__":
    setup_logging()
    transformer = BooksRawTransformer()
    success = transformer.execute()
    import sys
    sys.exit(0 if success else 1)