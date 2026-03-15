#!/usr/bin/env python3
"""
📖 UTILISATION PRATIQUE - Guide d'utilisation du refactoring appliqué

Ce fichier montre comment utiliser les jobs refactorisés
"""

import sys
from pathlib import Path

# Add project root
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def example_1_simple_usage():
    """EXEMPLE 1: Utilisation simple"""
    print("\n" + "="*60)
    print("EXEMPLE 1: Utilisation Simple")
    print("="*60)
    
    from core.logger import setup_logging
    from core.config import ConfigManager
    
    # Setup une seule fois
    setup_logging()
    
    # Récupérer la config centralisée
    config = ConfigManager.get_instance()
    
    print(f"""
Configuration centralisée:
  - MinIO endpoint: {config.minio.endpoint}
  - MinIO bucket: {config.minio.bucket}
  - Bronze layer: {config.paths.bronze_layer}
  - Silver layer: {config.paths.silver_layer}
  - DB host: {config.database.host}
    """)


def example_2_extract_job():
    """EXEMPLE 2: Exécuter un job d'extraction"""
    print("\n" + "="*60)
    print("EXEMPLE 2: Job d'Extraction (CSV)")
    print("="*60)
    
    from jobs.extract.extract_csv import CSVExtractor
    
    print("""
# Créer l'extracteur
extractor = CSVExtractor()

# Exécuter
success = extractor.execute()

# Vérifier le résultat
if success:
    print("✅ CSV extraction complete")
else:
    print("❌ CSV extraction failed")
    """)
    
    # Demo
    print("\n📌 Demo (sans exécution réelle):")
    extractor = CSVExtractor()
    print(f"   Instance créée: {extractor.name}")
    print(f"   Type: {extractor.__class__.__name__}")
    print(f"   Methode execute() disponible: {hasattr(extractor, 'execute')}")


def example_3_load_job():
    """EXEMPLE 3: Exécuter un job de chargement"""
    print("\n" + "="*60)
    print("EXEMPLE 3: Job de Chargement (JSON → MinIO)")
    print("="*60)
    
    from jobs.load.load_to_minio import BooksRawLoader
    
    print("""
# Créer le loader
loader = BooksRawLoader()

# Exécuter
success = loader.execute()

# Vérifier
if success:
    print("✅ Files uploaded to MinIO")
else:
    print("❌ Upload failed")
    """)
    
    # Demo
    print("\n📌 Demo:")
    loader = BooksRawLoader()
    print(f"   Instance créée: {loader.name}")
    print(f"   MinIO client intégré: {hasattr(loader, 'minio')}")


def example_4_scraper():
    """EXEMPLE 4: Job de scraping"""
    print("\n" + "="*60)
    print("EXEMPLE 4: Web Scraper (books.toscrape.com)")
    print("="*60)
    
    from jobs.extract.scrape_books import BookScraper
    
    print("""
# Créer le scraper
scraper = BookScraper()

# Scraper scraperá 50 pages max (configurable)
# Sauvegarde les résultats en JSON
success = scraper.execute()

# Résultat
if success:
    print("✅ Books scraped and saved")
    """)
    
    # Demo
    print("\n📌 Demo:")
    scraper = BookScraper()
    print(f"   Instance créée: {scraper.name}")
    print(f"   URL: {scraper.BASE_URL.format(1)}")
    print(f"   Max pages: {scraper.MAX_PAGES}")


def example_5_pipeline():
    """EXEMPLE 5: Pipeline complet"""
    print("\n" + "="*60)
    print("EXEMPLE 5: Pipeline ETL Complet")
    print("="*60)
    
    from core.base import Pipeline
    from jobs.extract.extract_csv import CSVExtractor
    from jobs.load.load_to_minio import BooksRawLoader
    
    print("""
# Créer le pipeline
pipeline = Pipeline("Books ETL")

# Ajouter les stages
pipeline.add(CSVExtractor())
pipeline.add(BooksRawLoader())

# Exécuter (s'arrête si une étape échoue)
success = pipeline.execute()

# Résultat
if success:
    print("✅ Full pipeline successful!")
else:
    print("❌ Pipeline failed at some stage")
    """)
    
    # Demo
    print("\n📌 Demo:")
    pipeline = Pipeline("Books ETL Demo")
    pipeline.add(CSVExtractor())
    pipeline.add(BooksRawLoader())
    
    print(f"   Pipeline créé: {pipeline.name}")
    print(f"   Nombre de stages: {len(pipeline.stages)}")
    for i, stage in enumerate(pipeline.stages, 1):
        print(f"     {i}. {stage.name}")


def example_6_migration_pattern():
    """EXEMPLE 6: Pattern de migration pour un nouveau job"""
    print("\n" + "="*60)
    print("EXEMPLE 6: Pattern de Migration (Template)")
    print("="*60)
    
    print("""
# POUR UN EXTRACTEUR:
from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

class MyExtractor(Extractor):
    def __init__(self):
        super().__init__(name="MyExtractor")
        self.config = ConfigManager.get_instance()
    
    def extract(self) -> bool:
        try:
            self.log_info("Extracting data...")
            # Votre logique ici
            self.log_info("✓ Extract complete")
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

# POUR UN LOADER:
from core.base import Loader
from core.minio_client import MinIOClient

class MyLoader(Loader):
    def __init__(self):
        super().__init__(name="MyLoader")
        self.config = ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            # Votre logique ici
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

# POUR UN TRANSFORMEUR:
from core.base import Transformer
from core.spark_manager import SparkManager

class MyTransformer(Transformer):
    def __init__(self):
        super().__init__(name="MyTransformer")
        self.spark_manager = SparkManager.get_instance()
    
    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            # Votre logique Spark ici
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

# UTILISATION:
if __name__ == "__main__":
    setup_logging()
    job = MyExtractor()  # ou MyLoader, MyTransformer
    import sys
    sys.exit(0 if job.execute() else 1)
    """)


def example_7_config_usage():
    """EXEMPLE 7: Utiliser ConfigManager"""
    print("\n" + "="*60)
    print("EXEMPLE 7: ConfigManager Avancé")
    print("="*60)
    
    from core.config import ConfigManager
    
    config = ConfigManager.get_instance()
    
    print(f"""
Configuration disponible:

1. MinIO
   config.minio.endpoint  = {config.minio.endpoint}
   config.minio.bucket    = {config.minio.bucket}
   config.minio.access_key = **** (caché)
   config.minio.use_ssl   = {config.minio.use_ssl}

2. Database
   config.database.host     = {config.database.host}
   config.database.port     = {config.database.port}
   config.database.database = {config.database.database}
   config.database.user     = {config.database.user}

3. Spark
   config.spark.app_name          = {config.spark.app_name}
   config.spark.shuffle_partitions = {config.spark.shuffle_partitions}
   config.spark.jar_path          = {config.spark.jar_path[:50]}...

4. Paths
   config.paths.csv_source   = {config.paths.csv_source}
   config.paths.bronze_layer = {config.paths.bronze_layer}
   config.paths.silver_layer = {config.paths.silver_layer}
    """)


def example_8_logging_usage():
    """EXEMPLE 8: Logging standardisé"""
    print("\n" + "="*60)
    print("EXEMPLE 8: Logging Standardisé")
    print("="*60)
    
    from core.logger import setup_logging, get_logger
    import logging
    
    print("""
# Setup une seule fois
setup_logging(level=logging.INFO)

# Utiliser partout
logger = get_logger(__name__)

logger.debug("Debug message - detailed info")
logger.info("Info message - important milestone")
logger.warning("Warning message - potential issue")
logger.error("Error message - something went wrong")

# Tous les logs sont:
# - Avec timestamp
# - Avec niveau
# - Avec module name
# - Format standardisé
    """)


def example_9_minio_advanced():
    """EXEMPLE 9: MinIOClient avancé"""
    print("\n" + "="*60)
    print("EXEMPLE 9: MinIOClient Avancé")
    print("="*60)
    
    from core.minio_client import MinIOClient
    from core.config import ConfigManager
    
    config = ConfigManager.get_instance()
    client = MinIOClient(config.minio)
    
    print("""
client = MinIOClient(config.minio)

# Bucket operations
client.ensure_bucket_exists()
client.bucket_exists("my-bucket")

# Upload operations
client.upload_file(Path("local.json"), "s3/path/file.json")
uploaded, total = client.upload_directory(
    local_dir=Path("./data"),
    s3_prefix="data/",
    extension="*.json"
)

# Download operations
client.download_file("s3/key", Path("local_file"))

# List operations
objects = client.list_objects(prefix="data/")
folder_has_files = client.folder_exists("bucket", "prefix/")

# Delete operations
client.delete_object("s3/key")

# Tous les retours incluent du logging automatique
    """)


def example_10_migration_checklist():
    """EXEMPLE 10: Checklist de migration"""
    print("\n" + "="*60)
    print("EXEMPLE 10: Checklist Complète de Migration")
    print("="*60)
    
    print("""
POUR CHAQUE JOB À MIGRER:

□ PRÉPARER
  □ Lire le code ancien
  □ Identifier le type (Extract/Load/Transform)
  □ Noter la logique métier

□ CRÉER LA CLASSE
  □ Hériter de la bonne classe (Extractor/Loader/Transformer)
  □ Implémenter la méthode requise (extract/load/transform)
  □ Ajouter docstring

□ REMPLACER LA CONFIG
  □ os.getenv() → self.config.minio / self.config.database
  □ Hardcoded paths → self.config.paths

□ REMPLACER LE LOGGING
  □ logging.info() → self.log_info()
  □ logging.error() → self.log_error()
  □ Supprimer logging.basicConfig()

□ METTRE À JOUR LE MAIN
  □ setup_logging() au démarrage
  □ job = MyJob()
  □ success = job.execute()
  □ sys.exit(0 if success else 1)

□ TESTER
  □ python jobs/.../my_job.py
  □ Vérifier les logs
  □ Vérifier les résultats

□ METTRE À JOUR LE DAG
  □ from jobs... import MyJob
  □ def run_job():
  │   job = MyJob()
  │   return job.execute()
  └─ PythonOperator(python_callable=run_job)

TEMPS ESTIMÉ PAR JOB: 10-20 minutes
    """)


def show_status():
    """Affiche le statut de migration"""
    print("\n" + "="*60)
    print("STATUS DE MIGRATION")
    print("="*60)
    
    statuses = {
        "Extract": {
            "extract_csv.py": "✅ DONE",
            "extract_sql.py": "⏳ TODO",
            "scrape_books.py": "✅ DONE",
        },
        "Load": {
            "load_to_minio.py": "✅ DONE",
            "load_csv_to_minio.py": "⏳ TODO",
            "load_sql_to_minio.py": "⏳ TODO",
        },
        "Transform": {
            "transform_books.py": "⏳ TODO",
            "transform_csv_books.py": "⏳ TODO",
            "transform_sql_books.py": "⏳ TODO",
        },
    }
    
    total_jobs = 0
    done_jobs = 0
    
    for category, jobs in statuses.items():
        print(f"\n{category}:")
        for job, status in jobs.items():
            print(f"  {status} {job}")
            total_jobs += 1
            if "DONE" in status:
                done_jobs += 1
    
    print(f"\nTotal: {done_jobs}/{total_jobs} jobs migrated ({100*done_jobs//total_jobs}%)")


def main():
    """Affiche tous les exemples"""
    print("\n" + "📖" * 30)
    print("GUIDE D'UTILISATION PRATIQUE")
    print("📖" * 30)
    
    examples = [
        ("Setup Simple", example_1_simple_usage),
        ("Job d'Extraction", example_2_extract_job),
        ("Job de Chargement", example_3_load_job),
        ("Web Scraper", example_4_scraper),
        ("Pipeline Complet", example_5_pipeline),
        ("Pattern de Migration", example_6_migration_pattern),
        ("ConfigManager", example_7_config_usage),
        ("Logging Standardisé", example_8_logging_usage),
        ("MinIOClient Avancé", example_9_minio_advanced),
        ("Checklist Migration", example_10_migration_checklist),
    ]
    
    # Menu
    print("\n📚 Choisissez un exemple à afficher:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    print("  0. Tout afficher")
    print("  q. Quitter")
    
    # Afficher le status
    show_status()
    
    print("\n" + "="*60)
    print("POUR EXÉCUTER UN JOB REFACTORISÉ:")
    print("="*60)
    print("""
$ cd /path/to/project
$ python jobs/extract/extract_csv.py
$ python jobs/load/load_to_minio.py
$ python jobs/extract/scrape_books.py
    """)
    
    print("\nPour plus de détails:")
    print("  - Consultar MIGRATION_GUIDE.md")
    print("  - Consultar BEST_PRACTICES.md")
    print("  - Consultar QUICK_START.md")


if __name__ == "__main__":
    main()
