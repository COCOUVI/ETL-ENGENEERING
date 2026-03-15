"""
🚀 QUICK START - Guide rapide pour utiliser le refactoring

Ce fichier vous montre comment commencer immédiatement
"""

# =============================================================================
# 📌 FICHIERS CRÉÉS
# =============================================================================

"""
Répertoire 'core/' (Architecture OOP centralisée):
├── __init__.py                  - Exports de tous les modules core
├── config.py                    - ConfigManager (Singleton) pour configuration
├── minio_client.py              - MinIOClient pour toutes opérations MinIO
├── spark_manager.py             - SparkManager pour SparkSession
├── logger.py                    - Logging centralisé
├── base.py                      - Abstract Base Classes (Extractor, Transformer, Loader, Pipeline)
└── implementations.py           - Implémentations d'exemple

Documentation:
├── REFACTORING_REPORT.md        - Rapport d'analyse du projet
├── REFACTORING_GUIDE.md         - Guide complet du refactoring
├── REFACTORING_EXAMPLES.md      - Exemples de code avant/après
├── BEST_PRACTICES.md            - Best practices et patterns
└── QUICK_START.md               - Ce fichier!
"""

# =============================================================================
# 🎯 OPTIONS DE DÉMARRAGE
# =============================================================================

# OPTION 1: Utiliser les classes simples (5 minutes)
# ============================================================================

print("=== OPTION 1: Classes réutilisables ===\n")

from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.logger import setup_logging, get_logger
import logging

# 1. Setup logging (à faire une fois au démarrage)
setup_logging(level=logging.INFO)
logger = get_logger(__name__)

# 2. Charger la config
config = ConfigManager.get_instance()
logger.info(f"Config loaded: {config}")

# 3. Utiliser MinIO partout
minio = MinIOClient(config.minio)
minio.ensure_bucket_exists()

# Done! Pas besoin de refaire du get_s3_client() ou ensure_bucket_exists()
# Utilisez juste `minio` partout dans votre code


# OPTION 2: Classes ETL avec héritage (15 minutes)
# ============================================================================

print("\n=== OPTION 2: Classes ETL (Extractor/Loader/Transformer) ===\n")

from core.base import Extractor, Loader, Transformer, Pipeline
from pathlib import Path

# Créer une classe extracteur
class MyDataExtractor(Extractor):
    def __init__(self):
        super().__init__(name="MyDataExtractor")
        self.config = ConfigManager.get_instance()
    
    def extract(self) -> bool:
        try:
            self.log_info("Extracting data...")
            
            # Votre logique ici
            data = ["item1", "item2", "item3"]
            
            self.log_info(f"✓ Extracted {len(data)} items")
            return True
        except Exception as e:
            self.log_error(f"Error: {e}")
            return False

# Créer une classe loader
class MyDataLoader(Loader):
    def __init__(self):
        super().__init__(name="MyDataLoader")
        self.minio = MinIOClient()
    
    def load(self) -> bool:
        try:
            self.log_info("Loading data...")
            
            # Votre logique ici
            self.minio.ensure_bucket_exists()
            
            self.log_info("✓ Load completed")
            return True
        except Exception as e:
            self.log_error(f"Error: {e}")
            return False

# Composer avec Pipeline pour l'orchestration
pipeline = Pipeline("My Data Pipeline")
pipeline.add(MyDataExtractor())
pipeline.add(MyDataLoader())

# Exécuter
success = pipeline.execute()


# OPTION 3: Pipeline Complet (20 minutes)
# ============================================================================

print("\n=== OPTION 3: Pipeline complet avec tous les composants ===\n")

from core.spark_manager import SparkManager

class MyCompleteExtractor(Extractor):
    def extract(self) -> bool:
        self.log_info("Extraction...")
        return True

class MyCompleteTransformer(Transformer):
    def transform(self) -> bool:
        spark_mgr = SparkManager.get_instance()
        spark = spark_mgr.get_session()
        
        self.log_info("Transforming with Spark...")
        return True

class MyCompleteLoader(Loader):
    def load(self) -> bool:
        minio = MinIOClient()
        self.log_info("Loading to MinIO...")
        return True

# Build pipeline
pipeline = Pipeline("Complete ETL")
pipeline.add(MyCompleteExtractor())
pipeline.add(MyCompleteTransformer())
pipeline.add(MyCompleteLoader())

# Execute
success = pipeline.execute()


# =============================================================================
# 📚 EXEMPLES PRATIQUES
# =============================================================================

# EXEMPLE 1: Upload simple de fichiers
# ============================================================================

def upload_files_example():
    """Upload des fichiers vers MinIO"""
    
    from core.minio_client import MinIOClient
    from pathlib import Path
    
    minio = MinIOClient()
    minio.ensure_bucket_exists()
    
    # Upload un seul fichier
    minio.upload_file(
        local_path=Path("data.json"),
        s3_key="raw/data.json"
    )
    
    # Upload un répertoire entier
    uploaded, total = minio.upload_directory(
        local_dir=Path("./data/raw"),
        s3_prefix="raw/",
        extension="*.json"
    )
    print(f"Uploaded {uploaded}/{total} files")


# EXEMPLE 2: Spark avec configuration centralisée
# ============================================================================

def spark_example():
    """Utiliser Spark avec config centralisée"""
    
    from core.spark_manager import SparkManager
    
    manager = SparkManager.get_instance()
    spark = manager.get_session()
    
    # Lire depuis S3A
    df = spark.read.csv("s3a://bucket/data.csv", header=True)
    
    # Traiter
    df_clean = df.filter(df.price > 0)
    
    # Écrire
    df_clean.write.parquet("s3a://bucket/output")


# EXEMPLE 3: Configuration simple
# ============================================================================

def config_example():
    """Accéder à la configuration"""
    
    from core.config import ConfigManager
    
    config = ConfigManager.get_instance()
    
    # MinIO
    print(f"MinIO endpoint: {config.minio.endpoint}")
    print(f"MinIO bucket: {config.minio.bucket}")
    
    # Database
    print(f"DB host: {config.database.host}")
    print(f"DB port: {config.database.port}")
    
    # Paths
    print(f"Bronze layer: {config.paths.bronze_layer}")
    print(f"Silver layer: {config.paths.silver_layer}")
    
    # Spark
    print(f"Spark app: {config.spark.app_name}")


# EXEMPLE 4: Logging avec standardisation
# ============================================================================

def logging_example():
    """Utiliser le logging centralisé"""
    
    from core.logger import setup_logging, get_logger
    import logging
    
    # Setup (une fois au démarrage)
    setup_logging(level=logging.INFO)
    
    # Obtenir un logger par module
    logger = get_logger(__name__)
    
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message", exc_info=True)


# =============================================================================
# 🔄 MIGRATION WORKFLOW
# =============================================================================

"""
Pour migrer un job existant:

1. LIRE l'ancien code:
   - Identifier les fonctions dupliquées
   - Identifier les config dispersées
   
2. CRÉER une nouvelle classe:
   class MyExtractor(Extractor):
       def extract(self) -> bool:
           # Logique métier ici
   
3. REMPLACER les anciens appels:
   ❌ AVANT: get_s3_client() + ensure_bucket_exists()
   ✅ APRÈS: MinIOClient(config.minio)
   
   ❌ AVANT: logging.basicConfig() partout
   ✅ APRÈS: setup_logging() une fois + get_logger()
   
   ❌ AVANT: os.getenv(...) partout
   ✅ APRÈS: config = ConfigManager.get_instance()

4. TESTER:
   - python -m pytest tests/test_my_job.py
   - Vérifier logs
   - Vérifier MinIO uploads
   
5. DÉPLOYER:
   - Mettre à jour DAG
   - Redémarrer Airflow
   - Monitorer logs

ESTIMÉ: 10-30 minutes par job selon complexité
"""

# =============================================================================
# ✨ TIPS BONUS
# =============================================================================

"""
1. TYPE HINTS: Ajoutez les partout
   ❌ def process(data):
   ✅ def process(data: list[dict]) -> bool:

2. DOCSTRINGS: Utilisez Google style
   def process(data):
       '''
       Processus les données.
       
       Args:
           data: Liste de dicts
       
       Returns:
           bool: Succès ou non
       '''

3. ERROR HANDLING: Spécifique pas generic
   ❌ except: pass
   ✅ except ClientError as e: logger.error(...)

4. LOGGING: Informatif et hiérarchisé
   logger.info("Starting ETL")
   logger.debug("Processing record 1")
   logger.warning("Missing field")
   logger.error("Failed to upload")

5. TESTING: Créez des tests pour chaque classe
   - test_config.py
   - test_minio_client.py
   - test_extractors.py
   - test_loaders.py
   - test_transformers.py

6. PERFORMANCE:
   - Utilisez lazy initialization (@property)
   - Utilisez Singleton pour resources coûteuses
   - Composez avec Pipeline pour orchestration efficace

7. MONITORING:
   - Logs détaillés mais pas trop verbeux
   - Utilisez metrics/counters
   - Ajouter timestamps aux imports
"""

# =============================================================================
# 🆘 TROUBLESHOOTING
# =============================================================================

"""
PROBLÈME: ImportError: No module named 'core'
SOLUTION: cd /path/to/project && python -m package

PROBLÈME: ConfigManager.get_instance() retourne None
SOLUTION: Vérifier .env existe et contient MINIO_ROOT_USER, MINIO_ROOT_PASSWORD

PROBLÈME: MinIO connection refused
SOLUTION: Vérifier MINIO_ENDPOINT dans .env (ex: http://minio:9000)

PROBLÈME: Spark session not available
SOLUTION: Appeler SparkManager.get_instance().get_session() d'abord

PROBLÈME: Logging ne s'affiche pas
SOLUTION: setup_logging() doit être appelé au démarrage

PROBLÈME: Bucket not found
SOLUTION: Appeler minio.ensure_bucket_exists() au début
"""

# =============================================================================
# 📖 PROCHAINES NIVEAUX
# =============================================================================

print("""
✅ NIVEAU 1: Classes simples (core modules)
   - ConfigManager
   - MinIOClient
   - SparkManager
   - Logger

⏳ NIVEAU 2: Classes ETL avec héritage
   - Extractor
   - Transformer
   - Loader
   - Pipeline

⏳ NIVEAU 3: Design Patterns avancés
   - Factory patterns
   - Strategy patterns
   - Observer patterns
   - Dependency Injection

⏳ NIVEAU 4: Testing & CI/CD
   - Unit tests
   - Integration tests
   - Test coverage
   - GitHub Actions
   
⏳ NIVEAU 5: Documentation & API
   - OpenAPI/Swagger
   - API REST pour orchestration
   - Web dashboard
   - Monitoring
""")

# =============================================================================
# 🎓 APPRENTISSAGE
# =============================================================================

print("""
Lire dans cet ordre:
1. REFACTORING_REPORT.md          → Comprendre les problèmes
2. REFACTORING_GUIDE.md           → Comprendre la solution
3. REFACTORING_EXAMPLES.md        → Voir examples avant/après
4. core/implementations.py        → Voir code refactorisé complet
5. BEST_PRACTICES.md              → Bonnes pratiques
6. Ce fichier (QUICK_START.md)   → Démarrer tout de suite

Puis commencer la migration d'1-2 jobs pour pratiquer!
""")

if __name__ == "__main__":
    print(__doc__)
    print("\n✅ Tous les modules core sont prêts à l'usage!")
    print("📚 Consultez les fichiers .md pour plus de détails")
    print("🚀 Commencez à refactoriser dès maintenant!")
