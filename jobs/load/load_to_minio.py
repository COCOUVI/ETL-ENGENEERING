"""
PHASE ETL : LOAD - Chargement des données RAW vers MinIO (S3 compatible)

Refactored with OOP pattern:
- Utilise MinIOClient centralisé (plus de duplication)
- Utilise ConfigManager (config unique)
- Utilise Loader base class (contrat ETL)
- Utilise Logger centralisé
"""

from pathlib import Path
from typing import Optional, Tuple

from core.base import Loader
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.logger import get_logger, setup_logging

logger = get_logger(__name__)


class BooksRawLoader(Loader):
    """
    Loader pour fichiers RAW (JSON) de livres vers MinIO
    
    Rôle:
    - Parcourir les fichiers RAW locaux (JSON)
    - Reproduire la structure dans MinIO
    - Charger les fichiers vers S3 compatible
    
    Remplace l'ancien load_to_minio.py procédural
    """
    
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Args:
            config: ConfigManager instance (utilise singleton si None)
        """
        super().__init__(name="BooksRawLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        """
        Upload tous les fichiers JSON du dossier books vers MinIO
        
        Returns:
            bool: True si tous les fichiers uploadés, False sinon
        """
        try:
            # Étape 1: Vérifier/créer le bucket
            self.minio.ensure_bucket_exists()
            
            # Étape 2: Déterminer le chemin source
            source_dir = Path(self.config.paths.bronze_layer) / "books"
            self.log_info(f"Source directory: {source_dir}")
            
            if not source_dir.exists():
                self.log_warning(f"Directory not found: {source_dir}")
                return False
            
            # Étape 3: Upload tous les fichiers JSON
            uploaded, total = self.minio.upload_directory(
                local_dir=source_dir,
                s3_prefix="books/",
                extension="*.json"
            )
            
            # Étape 4: Log résultats
            success = uploaded == total
            self.log_info(f"✓ Upload results: {uploaded}/{total} files successful")
            
            return success
            
        except Exception as e:
            self.log_error(f"Load failed: {e}")
            return False


if __name__ == "__main__":
    # Setup logging (une fois au démarrage)
    setup_logging()
    
    # Créer et exécuter le loader
    loader = BooksRawLoader()
    success = loader.execute()
    
    # Exit avec code approprié
    import sys
    sys.exit(0 if success else 1)
