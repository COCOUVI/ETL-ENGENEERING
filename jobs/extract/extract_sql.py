"""
PHASE ETL : EXTRACT - Source PostgreSQL
Sauvegarde en format Parquet
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import psycopg2
import os
from typing import List, Dict, Optional

from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger, setup_logging


# ============================================================
# CONFIGURATION
# ============================================================


BRONZE_OUTPUT_PATH = Path("/opt/airflow/data/bronze-layer/sql")
setup_logging()
logger = get_logger(__name__)


# ============================================================
# EXTRACT
# ============================================================

class SQLExtractor(Extractor):
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Args:
            config: ConfigManager instance (utilise singleton si None)
        """
        super().__init__(name="SQLExtractor")
        self.config = config or ConfigManager.get_instance()
        self.db_cfg = self.config.database

    def extract(self) -> bool:
        """
        Lit les tables PostgreSQL et les sauvegarde en Parquet dans le bronze-layer
        """
        conn = None
        try:
            logger.info(f"Connecting to PostgreSQL: {self.db_cfg.host}:{self.db_cfg.port}/{self.db_cfg.database}")
            conn = psycopg2.connect(
                host=self.db_cfg.host,
                port=self.db_cfg.port,
                database=self.db_cfg.database,
                user=self.db_cfg.user,
                password=self.db_cfg.password
            )
            logger.info("PostgreSQL connected successfully")

            tables = ['authors', 'categories', 'books']
            total_rows = 0
            for table_name in tables:
                logger.info(f"Reading table: {table_name}")
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)
                rows = len(df)
                total_rows += rows
                logger.info(f"Table {table_name} loaded: {rows} rows, {len(df.columns)} columns")

                table_output_path = BRONZE_OUTPUT_PATH / table_name
                table_output_path.mkdir(parents=True, exist_ok=True)

                output_file = table_output_path / f"{table_name}_{datetime.utcnow().date()}.parquet"
                df.to_parquet(
                    output_file,
                    index=False,
                    engine='pyarrow',
                    coerce_timestamps='us',
                    allow_truncated_timestamps=True
                )
                file_size = output_file.stat().st_size / 1024  # en KB
                logger.info(f"Saved to {output_file} ({file_size:.2f} KB)")

            logger.info(f"✓ Extraction completed: {total_rows} total rows from {len(tables)} tables")
            return True

        except psycopg2.Error as e:
            logger.error(f"PostgreSQL error: {e}")
            return False
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return False
        finally:
            if conn:
                conn.close()
                logger.info("PostgreSQL connection closed")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    extractor = SQLExtractor()
    success = extractor.execute()
    if not success:
        exit(1)