"""
📚 EXAMPLES de Refactoring - Comment utiliser les classes centralisées

Ce fichier montre comment convertir le code ancien (procédural)
vers le nouveau code (OOP avec classes réutilisables)
"""

# =============================================================================
# ✅ EXEMPLE 1: Scrapers (Ancien vs Nouveau)
# =============================================================================

# ❌ AVANT (procédural, dupliqué partout)
"""
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        ...
    )

def ensure_bucket_exists():
    ...

def upload_file():
    ...
"""

# ✅ APRÈS (OOP réutilisable)
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.logger import get_logger
from core.base import Extractor, Pipeline

logger = get_logger(__name__)


class BookScraper(Extractor):
    """Scraper de livres - Utilise la nouvelle architecture"""
    
    def __init__(self):
        super().__init__(name="BookScraper")
        self.config = ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def extract(self) -> bool:
        """Implémentation de l'extraction"""
        import requests
        from bs4 import BeautifulSoup
        import json
        from datetime import datetime
        from pathlib import Path
        
        try:
            # Scraping logic
            base_url = "https://books.toscrape.com/catalogue/page-{}.html"
            all_books = []
            
            for page in range(1, 50):
                response = requests.get(base_url.format(page))
                if not response.ok:
                    break
                
                soup = BeautifulSoup(response.text, "html.parser")
                books = soup.select("article.product_pod")
                
                if not books:
                    break
                
                for article in books:
                    all_books.append({
                        "title": article.h3.a["title"],
                        "price": article.select_one(".price_color").text,
                        "rating": article.p["class"][1],
                        "scraped_at": datetime.utcnow().isoformat()
                    })
            
            # Sauvegarder en local
            raw_path = Path(self.config.paths.bronze_layer) / "books"
            raw_path.mkdir(parents=True, exist_ok=True)
            
            output_file = raw_path / f"books_{datetime.utcnow().date()}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_books, f, indent=2, ensure_ascii=False)
            
            self.log_info(f"Scraping réussi: {len(all_books)} livres")
            return True
            
        except Exception as e:
            self.log_error(f"Scraping échoué: {e}")
            return False


# =============================================================================
# ✅ EXEMPLE 2: Loader MinIO (Ancien vs Nouveau)
# =============================================================================

# ❌ AVANT (5 fichiers load_*.py avec du code dupliqué)
"""
def get_s3_client():
    ...

def ensure_bucket_exists():
    ...

def upload_raw_files():
    s3 = get_s3_client()
    ensure_bucket_exists(s3)
    
    for file_path in RAW_BASE_PATH.rglob("*.json"):
        relative_path = file_path.relative_to(RAW_BASE_PATH)
        s3_key = f"books/{relative_path}"
        s3.upload_file(...)
"""

# ✅ APRÈS (Réutilisable et sans duplication)
from core.base import Loader
from pathlib import Path


class MinIOLoader(Loader):
    """Chargeur MinIO générique"""
    
    def __init__(
        self,
        local_dir: Path,
        s3_prefix: str,
        file_pattern: str = "*.json",
    ):
        """
        Args:
            local_dir: Dossier source
            s3_prefix: Préfixe S3 (ex: 'books/', 'sql/')
            file_pattern: Pattern de fichiers à loader
        """
        super().__init__(name=f"MinIOLoader-{s3_prefix}")
        self.local_dir = Path(local_dir)
        self.s3_prefix = s3_prefix
        self.file_pattern = file_pattern
        self.minio = MinIOClient()
    
    def load(self) -> bool:
        """Charge les fichiers vers MinIO"""
        try:
            self.minio.ensure_bucket_exists()
            
            uploaded, total = self.minio.upload_directory(
                local_dir=self.local_dir,
                s3_prefix=self.s3_prefix,
                extension=self.file_pattern,
            )
            
            self.log_info(f"Upload: {uploaded}/{total} fichiers")
            return uploaded == total
            
        except Exception as e:
            self.log_error(f"Chargement MinIO échoué: {e}")
            return False


# Usage:
# loader = MinIOLoader(
#     local_dir="/opt/airflow/data/bronze-layer/books",
#     s3_prefix="books/",
#     file_pattern="*.json"
# )
# loader.execute()


# =============================================================================
# ✅ EXEMPLE 3: Transform Spark (Ancien vs Nouveau)
# =============================================================================

# ❌ AVANT (create_spark_session dupliqué dans 2 fichiers)
"""
def create_spark_session():
    spark = SparkSession.builder \\
        .appName("TransformBooks") \\
        .config("spark.jars", jar_path) \\
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \\
        ... (20 lignes identiques)
    return spark.getOrCreate()

def transform_books():
    spark = create_spark_session()
    df = spark.read.csv("s3a://...")
    ...
"""

# ✅ APRÈS (Centralisé et réutilisable)
from core.spark_manager import SparkManager
from core.base import Transformer


class SparkTransformer(Transformer):
    """Transformateur Spark générique"""
    
    def __init__(self, source_path: str, output_path: str):
        """
        Args:
            source_path: Chemin des données source (s3a://...)
            output_path: Chemin de sortie (s3a://...)
        """
        super().__init__(name="SparkTransformer")
        self.source_path = source_path
        self.output_path = output_path
        self.spark_manager = SparkManager.get_instance()
    
    def transform(self) -> bool:
        """Transforme les données avec Spark"""
        try:
            spark = self.spark_manager.get_session()
            
            # Lecture
            self.log_info(f"Lecture: {self.source_path}")
            df = spark.read.format("csv").option("header", "true").load(self.source_path)
            
            # Transformation
            from pyspark.sql.functions import col, when
            df_clean = df.filter(col("price").isNotNull())
            
            # Écriture
            self.log_info(f"Écriture: {self.output_path}")
            df_clean.write.mode("overwrite").parquet(self.output_path)
            
            self.log_info(f"✓ Transformation complétée: {df_clean.count()} rows")
            return True
            
        except Exception as e:
            self.log_error(f"Transformation échouée: {e}")
            return False


# =============================================================================
# ✅ EXEMPLE 4: Pipeline complet
# =============================================================================

# Construction d'un pipeline ETL complet
def build_books_pipeline() -> Pipeline:
    """Construit le pipeline complet pour les livres"""
    
    pipeline = Pipeline("Books ETL")
    
    # Stage 1: Scraping
    pipeline.add(BookScraper())
    
    # Stage 2: Upload vers MinIO
    from pathlib import Path
    bronze_path = Path("/opt/airflow/data/bronze-layer/books")
    pipeline.add(MinIOLoader(
        local_dir=bronze_path,
        s3_prefix="books/",
        file_pattern="*.json"
    ))
    
    # Stage 3: Transform avec Spark
    pipeline.add(SparkTransformer(
        source_path="s3a://bronze-layer/books/",
        output_path="s3a://silver-layer/books_enriched/"
    ))
    
    return pipeline


# Usage:
# if __name__ == "__main__":
#     pipeline = build_books_pipeline()
#     success = pipeline.execute()
#     sys.exit(0 if success else 1)


# =============================================================================
# ✅ RÉSUMÉ DES AVANTAGES
# =============================================================================

"""
AVANT (Ancien code procédural):
- ❌ 500+ lignes dupliquées
- ❌ 8 copies de logging.basicConfig()
- ❌ 5 implémentations de get_s3_client()
- ❌ Difficile à maintenir
- ❌ Difficile à tester
- ❌ Couplage fort

APRÈS (Nouveau code OOP):
- ✅ DRY: Une seule implémentation de chaque classe
- ✅ OOP: Classes réutilisables et extensibles
- ✅ Composable: Pipeline = Composition de composants
- ✅ Testable: Chaque classe peut être testée isolément
- ✅ Maintenable: Changements en un seul endroit
- ✅ Découplé: Inversion de contrôle avec ConfigManager

MIGRATION PATH:
1. Importer les classes core
2. Créer une classe héritant de Extractor/Transformer/Loader
3. Implémenter la méthode extract()/transform()/load()
4. Utiliser ConfigManager et MinIOClient au lieu de code dupliqué
5. Composer avec Pipeline pour l'orchestration
"""
