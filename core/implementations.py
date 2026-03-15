"""
🔧 REFACTORED IMPLEMENTATIONS - Exemples complets et prêts à l'usage

Ce fichier montre des implémentations concrètes utilisant l'architecture OOP
"""

# =============================================================================
# 📦 EXEMPLE 1: Refactored load_to_minio.py
# =============================================================================

from pathlib import Path
from core.logger import get_logger
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.base import Loader

logger = get_logger(__name__)


class BooksRawLoader(Loader):
    """
    Loader pour fichiers RAW de livres vers MinIO
    
    Remplace l'ancien load_to_minio.py (procédural)
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="BooksRawLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        """
        Upload tous les fichiers JSON du dossier books vers MinIO
        """
        try:
            # Étape 1: Vérifier le bucket
            self.minio.ensure_bucket_exists()
            
            # Étape 2: Déterminer le chemin source
            source_dir = Path(self.config.paths.bronze_layer) / "books"
            self.log_info(f"Source directory: {source_dir}")
            
            if not source_dir.exists():
                self.log_warning(f"Directory not found: {source_dir}")
                return False
            
            # Étape 3: Upload les fichiers
            uploaded, total = self.minio.upload_directory(
                local_dir=source_dir,
                s3_prefix="books/",
                extension="*.json"
            )
            
            # Étape 4: Log résultats
            success = uploaded == total
            self.log_info(f"Upload results: {uploaded}/{total} files successful")
            
            return success
            
        except Exception as e:
            self.log_error(f"Load failed: {e}")
            return False


# =============================================================================
# 📦 EXEMPLE 2: Refactored extract_csv.py
# =============================================================================

import pandas as pd
from datetime import datetime
from core.logger import get_logger
from core.config import ConfigManager
from core.base import Extractor

logger = get_logger(__name__)


class CSVExtractor(Extractor):
    """
    Extracteur pour CSV source -> Bronze Layer
    
    Remplace l'ancien extract_csv.py (procédural)
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="CSVExtractor")
        self.config = config or ConfigManager.get_instance()
    
    def extract(self) -> bool:
        """
        Lit le CSV source et le sauvegarde en bronze-layer
        """
        try:
            csv_path = self.config.paths.csv_source
            self.log_info(f"Reading CSV from: {csv_path}")
            
            # Lecture du CSV
            df = pd.read_csv(
                csv_path,
                sep=",",
                encoding="latin-1",
                on_bad_lines="skip",
                quotechar='"'
            )
            self.log_info(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Création du dossier output
            output_dir = Path(self.config.paths.bronze_layer) / "books"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Sauvegarde en bronze-layer
            output_file = output_dir / f"books_csv_{datetime.utcnow().date()}.csv"
            df.to_csv(output_file, index=False, sep=";")
            self.log_info(f"Saved to: {output_file}")
            
            return True
            
        except Exception as e:
            self.log_error(f"Extraction failed: {e}")
            return False


# =============================================================================
# 📦 EXEMPLE 3: Refactored load_sql_to_minio.py
# =============================================================================

from core.logger import get_logger
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.base import Loader
from typing import Tuple

logger = get_logger(__name__)


class SQLRawLoader(Loader):
    """
    Loader générique pour fichiers SQL/CSV/WEB depuis bronze-layer
    
    Remplace les 3 anciens fichiers: load_to_minio.py, load_csv_to_minio.py, load_sql_to_minio.py
    Montre comment réutiliser le MÊME code pour différentes sources
    """
    
    def __init__(
        self,
        source_dirs: dict,  # {'sql': path, 'csv': path, 'web': path}
        config: ConfigManager = None
    ):
        """
        Args:
            source_dirs: Dict de {prefix: chemin local}
                Exemple: {'sql': '/data/bronze/sql', 'csv': '/data/bronze/csv'}
            config: ConfigManager instance
        """
        super().__init__(name="SQLRawLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
        self.source_dirs = source_dirs
    
    def load(self) -> bool:
        """
        Upload les fichiers de TOUTES les sources
        """
        try:
            self.minio.ensure_bucket_exists()
            
            total_uploaded = 0
            total_files = 0
            
            # Boucle sur toutes les sources
            for prefix, source_path in self.source_dirs.items():
                self.log_info(f"\n📁 Uploading {prefix} files...")
                
                uploaded, total = self._upload_from_source(
                    source_path,
                    prefix
                )
                
                total_uploaded += uploaded
                total_files += total
                
                result_text = f"{uploaded}/{total} files"
                self.log_info(f"   {prefix.upper()}: {result_text}")
            
            # Résumé final
            self.log_info(f"\n✓ Total: {total_uploaded}/{total_files} files uploaded")
            return total_uploaded == total_files
            
        except Exception as e:
            self.log_error(f"Load failed: {e}")
            return False
    
    def _upload_from_source(
        self,
        source_dir: str,
        prefix: str
    ) -> Tuple[int, int]:
        """Upload les fichiers d'une source vers MinIO"""
        
        source_path = Path(source_dir)
        
        if not source_path.exists():
            self.log_warning(f"Path does not exist: {source_path}")
            return 0, 0
        
        # Upload avec les extensions appropriées (.parquet, .csv)
        return self.minio.upload_directory(
            local_dir=source_path,
            s3_prefix=f"{prefix}/",
            extension="*.{parquet,csv,json}"
        )


# =============================================================================
# 📦 EXEMPLE 4: Refactored transform_books.py
# =============================================================================

from core.logger import get_logger
from core.config import ConfigManager
from core.spark_manager import SparkManager
from core.minio_client import MinIOClient
from core.base import Transformer
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, to_timestamp
)

logger = get_logger(__name__)


class BooksTransformer(Transformer):
    """
    Transformateur pour données livres (JSON)
    
    Remplace l'ancien transform_books.py (avec Spark dupliqué)
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="BooksTransformer")
        self.config = config or ConfigManager.get_instance()
        self.spark_manager = SparkManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def transform(self) -> bool:
        """
        Lit, nettoie et enrichit les données de livres
        """
        try:
            spark = self.spark_manager.get_session()
            
            # Étape 1: Lire les données brutes
            self.log_info("Reading raw data from S3...")
            df_raw = self._read_raw_data(spark)
            
            self.log_info(f"Raw data loaded: {df_raw.count()} rows")
            
            # Étape 2: Nettoyer et transformer
            self.log_info("Cleaning and transforming data...")
            df_clean = self._clean_books(df_raw)
            
            # Étape 3: Valider les données
            self.log_info("Validating clean data...")
            df_valid = self._validate_books(df_clean)
            
            valid_count = df_valid.count()
            self.log_info(f"Valid records: {valid_count}")
            
            # Étape 4: Sauvegarder en silver-layer
            self.log_info("Writing to silver-layer...")
            self._write_silver_layer(df_valid)
            
            return True
            
        except Exception as e:
            self.log_error(f"Transform failed: {e}")
            return False
    
    def _read_raw_data(self, spark: SparkSession):
        """Lit les fichiers JSON bruts de MinIO"""
        path = "s3a://bronze-layer/books/books_*.json"
        
        return spark.read \
            .option("multiline", "true") \
            .json(path)
    
    def _clean_books(self, df_raw):
        """Nettoie et type les colonnes"""
        
        df = df_raw
        
        # Renommer colonnes si nécessaire
        if "title" not in df.columns and "Title" in df.columns:
            df = df.withColumnRenamed("Title", "title")
        
        # Nettoyage des prix
        if "price" in df.columns:
            df = df.withColumn(
                "price",
                regexp_replace(col("price"), "[^0-9.]", "").cast("double")
            )
        
        # Suppression des colonnes null
        df = df.dropna(subset=["title"])
        
        return df
    
    def _validate_books(self, df):
        """Valide les règles métier"""
        
        # Exemple: prix doit être positif
        df = df.filter(col("price") > 0)
        
        # Disponibilité: Au moins un en stock
        if "availability" in df.columns:
            df = df.filter(col("availability").isNotNull())
        
        return df
    
    def _write_silver_layer(self, df):
        """Écrit les données en silver-layer"""
        
        output_path = "s3a://silver-layer/books_enriched"
        
        df.write \
            .mode("overwrite") \
            .partitionBy("scraped_at") \
            .parquet(output_path)
        
        self.log_info(f"Data written to: {output_path}")


# =============================================================================
# 📦 EXEMPLE 5: Pipeline Complet
# =============================================================================

from core.base import Pipeline


def build_books_etl_pipeline() -> Pipeline:
    """
    Construit le pipeline ETL complet pour les livres
    
    Montre comment composer plusieurs composants avec Pipeline
    """
    
    # Créer le pipeline
    pipeline = Pipeline("📚 Books ETL Pipeline")
    
    # Stage 1: Extraction CSV
    from jobs.extract.extract_csv import CSVExtractor
    csv_extractor = CSVExtractor()
    pipeline.add(csv_extractor)
    
    # Stage 2: Loader MinIO
    raw_loader = BooksRawLoader()
    pipeline.add(raw_loader)
    
    # Stage 3: Transform
    transformer = BooksTransformer()
    pipeline.add(transformer)
    
    return pipeline


# =============================================================================
# 📦 UTILISATION COMPLÈTE (main)
# =============================================================================

if __name__ == "__main__":
    import sys
    from core.logger import setup_logging
    import logging
    
    # 1. Setup logging
    setup_logging(level=logging.INFO)
    
    # 2. Créer et exécuter le pipeline
    pipeline = build_books_etl_pipeline()
    success = pipeline.execute()
    
    # 3. Exit avec code approprié
    sys.exit(0 if success else 1)


# =============================================================================
# 📊 COMPARAISON AVANT/APRÈS
# =============================================================================

"""
AVANT (Ancien Code):
- 500+ lignes de code procédural
- 5 fichiers load_*.py avec du code dupliqué
- 3 fichiers transform_*.py avec créations Spark dupliquées
- 8 logging.basicConfig() dispersés
- Pas de pattern réutilisable

APRÈS (Nouveau Code):
- Modules core: config.py, minio_client.py, spark_manager.py, logger.py, base.py
- Classes réutilisables: BooksRawLoader, CSVExtractor, BooksTransformer
- Pipeline pour orchestration
- Configuration centralisée
- Logging standardisé
- 100% OOP et Pattern-based

RÉSULTATS:
- ✅ Code dupliqué: 500+ → ~0 lignes
- ✅ Fichiers dupliqués: 8 → 0
- ✅ Réutilisabilité: Très bonne
- ✅ Testabilité: Facile (chaque classe indépendante)
- ✅ Maintenabilité: Excellente
"""
