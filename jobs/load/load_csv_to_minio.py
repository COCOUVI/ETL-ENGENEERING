"""
PHASE ETL : LOAD - Upload d'un fichier RAW CSV de livres vers MinIO

- Utilise ConfigManager pour la config (plus aucun os.getenv dans ce fichier)
- Utilise MinIOClient du core (DRY, testable, centralisé)
- Utilise Logger core (factorisé)
- Plus d'import dans les fonctions, tout en haut
"""
from pathlib import Path
from typing import Optional

from core.config import ConfigManager
from core.logger import get_logger, setup_logging
from core.minio_client import MinIOClient

logger = get_logger(__name__)

class BooksRawCSVLoader:
    """
    Loader pour upload d’un fichier RAW CSV "books" vers MinIO.
    - Rôle : uploader le dernier fichier CSV trouvé depuis le dossier bronze-layer vers MinIO, clé "books/{nom fichier}"
    """
    def __init__(self, config: Optional[ConfigManager] = None):
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        self.raw_path = Path(self.config.paths.bronze_layer) / "books"

    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            # Trouver le dernier CSV (par date modif, ou le premier trouvé)
            file_path = next(self.raw_path.rglob("*.csv"), None)
            if not file_path:
                logger.warning(f"Aucun fichier CSV trouvé dans {self.raw_path}")
                return False

            relative_path = file_path.relative_to(self.raw_path)
            s3_key = f"books/{relative_path}"
            logger.info(f"Uploading {file_path} → s3://{self.minio.bucket}/{s3_key}")
            self.minio.upload_file(local_file=str(file_path), s3_key=s3_key)
            logger.info(f"✓ Uploaded: {file_path} → s3://{self.minio.bucket}/{s3_key}")
            return True

        except Exception as e:
            logger.error(f"Failed to upload CSV: {e}")
            return False

if __name__ == "__main__":
    setup_logging()
    loader = BooksRawCSVLoader()
    success = loader.load()
    import sys
    sys.exit(0 if success else 1)