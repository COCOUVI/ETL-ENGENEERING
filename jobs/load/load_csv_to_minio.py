"""
PHASE ETL : LOAD - Upload d'un fichier RAW CSV de livres vers MinIO
"""
from pathlib import Path
from typing import Optional

from core.base import Loader
from core.config import ConfigManager
from core.logger import get_logger, setup_logging
from core.minio_client import MinIOClient

logger = get_logger(__name__)

class BooksRawCSVLoader(Loader):
    """
    Loader pour upload d'un fichier RAW CSV "books" vers MinIO.
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        super().__init__(name="BooksCSVLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        self.raw_path = Path(self.config.paths.bronze_layer) / "books"
        self.bucket_name = self.config.minio.bucket  # ← Stocker le bucket name

    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            file_path = next(self.raw_path.rglob("*.csv"), None)
            if not file_path:
                self.log_warning(f"Aucun fichier CSV trouvé dans {self.raw_path}")
                return False

            relative_path = file_path.relative_to(self.raw_path)
            s3_key = f"books/{relative_path}"
            
            self.log_info(f"Uploading {file_path} → s3://{self.bucket_name}/{s3_key}")  # ← FIX
            self.minio.upload_file(local_path=file_path, s3_key=s3_key)
            self.log_info(f"✓ Uploaded to s3://{self.bucket_name}/{s3_key}")  # ← FIX
            return True

        except Exception as e:
            self.log_error(f"Failed to upload CSV: {e}")
            return False

if __name__ == "__main__":
    setup_logging()
    loader = BooksRawCSVLoader()
    success = loader.execute()
    import sys
    sys.exit(0 if success else 1)