"""
PHASE ETL : LOAD - Chargement des données SQL (Parquet) vers MinIO (S3 compatible)

Refactored OOP:
- Utilise MinIOClient du core (plus de duplication de code S3)
- Utilise ConfigManager (config unique de toute l’app)
- Utilise Loader base class pour uniformité pipeline
- Logging centralisé/clean
"""

from pathlib import Path
from typing import Optional

from core.base import Loader
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.logger import get_logger, setup_logging

logger = get_logger(__name__)


class SQLMinioLoader(Loader):
    """
    Loader ETL pour charger les fichiers Parquet SQL dans MinIO (bronze/sql --> S3/sql/)
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Args:
            config: instance ConfigManager (sinon singleton)
        """
        super().__init__(name="SQLMinioLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        # Chemin absolu du dossier source SQL
        self.source_dir = Path(self.config.paths.bronze_layer) / "sql"

    def load(self) -> bool:
        """
        Charge tous les fichiers Parquet depuis le dossier SQL bronze-layer vers MinIO.
        Returns:
            bool: True si tous les fichiers uploadés, False sinon.
        """
        try:
            self.minio.ensure_bucket_exists()
            self.log_info(f"Source directory: {self.source_dir}")

            if not self.source_dir.exists():
                self.log_warning(f"SQL source directory not found: {self.source_dir}")
                return False

            uploaded, total = self.minio.upload_directory(
                local_dir=self.source_dir,
                s3_prefix="sql/",
                extension="*.parquet"
            )
            success = uploaded == total and total > 0
            self.log_info(f"✓ SQL Parquet upload: {uploaded}/{total} files successful")
            return success
        except Exception as e:
            self.log_error(f"Load SQL to MinIO failed: {e}")
            return False


if __name__ == "__main__":
    setup_logging()
    loader = SQLMinioLoader()
    success = loader.execute()
    # Exit correct selon succès
    import sys
    sys.exit(0 if success else 1)