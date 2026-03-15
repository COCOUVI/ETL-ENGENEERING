# ✅ CHECKLIST & BEST PRACTICES DE REFACTORING

## 🎯 Checklist de Migration

### Phase 1: Setup (✅ Complète)
- [x] Créer le répertoire `core/`
- [x] Créer `core/__init__.py`
- [x] Créer `core/config.py` (ConfigManager)
- [x] Créer `core/minio_client.py` (MinIOClient)
- [x] Créer `core/spark_manager.py` (SparkManager)
- [x] Créer `core/logger.py` (Logging centralisé)
- [x] Créer `core/base.py` (Abstract Base Classes)

### Phase 2: Documentation (✅ Complète)
- [x] `REFACTORING_REPORT.md` - Rapport d'analyse
- [x] `REFACTORING_GUIDE.md` - Guide complet
- [x] `REFACTORING_EXAMPLES.md` - Exemples de code
- [x] `core/implementations.py` - Implémentations concrètes

### Phase 3: Refactoring des Jobs (⏳ À faire)

#### Extract Jobs
- [ ] `jobs/extract/scrape_books.py`
  - [ ] Créer classe `BookScraper(Extractor)`
  - [ ] Utiliser `ConfigManager` pour config
  - [ ] Utiliser `get_logger()` pour logging
  - [ ] Tester avec pytest

- [ ] `jobs/extract/extract_csv.py`
  - [ ] Créer classe `CSVExtractor(Extractor)`
  - [ ] Utiliser `ConfigManager.paths.csv_source`
  - [ ] Tests unitaires

- [ ] `jobs/extract/extract_sql.py`
  - [ ] Créer classe `SQLExtractor(Extractor)`
  - [ ] Utiliser `ConfigManager.database` pour credentials
  - [ ] Tests unitaires

#### Load Jobs
- [ ] `jobs/load/load_to_minio.py`
  - [ ] Utiliser `MinIOLoader(Loader)`
  - [ ] Supprimer duplication

- [ ] `jobs/load/load_csv_to_minio.py`
  - [ ] Utiliser `MinIOLoader(Loader)`
  - [ ] Supprimer duplication

- [ ] `jobs/load/load_sql_to_minio.py`
  - [ ] Utiliser `MinIOLoader(Loader)`
  - [ ] Supprimer duplication

#### Transform Jobs
- [ ] `jobs/transform/transform_books.py`
  - [ ] Créer classe `BooksTransformer(Transformer)`
  - [ ] Utiliser `SparkManager.get_session()`
  - [ ] Tests unitaires

- [ ] `jobs/transform/transform_csv_books.py`
  - [ ] Créer classe `CSVBooksTransformer(Transformer)`
  - [ ] Utiliser `SparkManager.get_session()`
  - [ ] Tests unitaires

### Phase 4: Airflow DAGs (⏳ À faire)
- [ ] `dags/books_etl_pipeline.py`
  - [ ] Mettre à jour pour utiliser nouvelles classes
  - [ ] Utiliser `Pipeline` pour orchestration
  - [ ] Tester DAG

- [ ] `dags/etl_csv_pipeline.py`
  - [ ] Mettre à jour DAG
  - [ ] Tester

- [ ] `dags/etl_sql_pipeline.py`
  - [ ] Mettre à jour DAG
  - [ ] Tester

### Phase 5: Tests & Documentation (⏳ À faire)
- [ ] Créer `tests/test_config.py`
- [ ] Créer `tests/test_minio_client.py`
- [ ] Créer `tests/test_spark_manager.py`
- [ ] Créer `tests/test_extractors.py`
- [ ] Créer `tests/test_loaders.py`
- [ ] Créer `tests/test_transformers.py`
- [ ] Ajouter docstrings à tous les modules
- [ ] Mettre à jour README.md

---

## 🏆 Best Practices à Appliquer

### 1. Import Organization
```python
# ✅ BON: Ordre standard des imports
from abc import ABC, abstractmethod              # Standard library
from dataclasses import dataclass                # Standard library
from pathlib import Path                         # Standard library
from typing import Optional, Tuple              # Standard library

import pandas as pd                              # 3rd party
import boto3                                     # 3rd party

from core.config import ConfigManager            # Local imports
from core.logger import get_logger               # Local imports
```

### 2. Naming Conventions
```python
# ✅ Classes: PascalCase
class MinIOClient:
    pass

class BooksTransformer:
    pass

# ✅ Fonctions: snake_case
def setup_logging():
    pass

def get_logger():
    pass

# ✅ Constantes: UPPER_SNAKE_CASE
DEFAULT_BUCKET = "bronze-layer"
MAX_RETRIES = 3

# ✅ Variables privées: _leading_underscore
class MinIOClient:
    def __init__(self):
        self._client = None  # Private
        self.config = None   # Public
```

### 3. Type Hints Complets
```python
# ✅ BON: Type hints partout
def upload_directory(
    self,
    local_dir: Path,
    s3_prefix: str,
    extension: str = "*.json",
    bucket: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Upload un répertoire vers MinIO
    
    Args:
        local_dir: Chemin du répertoire local
        s3_prefix: Préfixe S3 (ex: 'books/')
        extension: Pattern des fichiers (ex: '*.json')
        bucket: Bucket cible (optionnel)
    
    Returns:
        Tuple[uploaded_count, total_count]
    """
    pass

# ❌ MAUVAIS: Pas de type hints
def upload_directory(local_dir, s3_prefix, extension, bucket):
    pass
```

### 4. Error Handling
```python
# ✅ BON: Spécifique et informatif
try:
    result = self.upload_file(file_path, s3_key)
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        self.log_warning(f"Bucket not found: {bucket}")
        self.create_bucket(bucket)
    else:
        self.log_error(f"S3 error: {e}")
        raise
except Exception as e:
    self.log_error(f"Unexpected error: {e}")
    return False

# ❌ MAUVAIS: Bare except
try:
    ...
except:
    pass
```

### 5. Logging
```python
# ✅ BON: Logging informatif via logger centralisé
from core.logger import get_logger

logger = get_logger(__name__)

logger.info("Starting ETL pipeline")
logger.warning("Bucket not found, creating...")
logger.error("Upload failed", exc_info=True)

# ❌ MAUVAIS: print() ou print et log mélangés
print("Starting ETL pipeline")
print("Error:", str(e))
```

### 6. Configuration Management
```python
# ✅ BON: Utiliser ConfigManager centralisé
from core.config import ConfigManager

config = ConfigManager.get_instance()
minio_cfg = config.minio
db_cfg = config.database

# ❌ MAUVAIS: Config dispersée
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
```

### 7. Docstrings
```python
# ✅ BON: Docstrings complètes (Google style)
def upload_file(
    self,
    local_path: Path,
    s3_key: str,
    bucket: Optional[str] = None,
) -> bool:
    """
    Upload un fichier vers MinIO.
    
    Args:
        local_path: Chemin du fichier local
        s3_key: Clé S3 (path dans le bucket)
        bucket: Bucket cible (optionnel)
    
    Returns:
        bool: True si upload réussi, False sinon
    
    Raises:
        ValueError: Si local_path n'existe pas
        ClientError: Si erreur S3 lors de l'upload
    
    Example:
        >>> client = MinIOClient(config.minio)
        >>> success = client.upload_file(
        ...     Path("local.json"),
        ...     "s3/path/file.json"
        ... )
    """
    pass

# ❌ MAUVAIS: Pas de docstring
def upload_file(self, local_path, s3_key):
    pass
```

### 8. Properties & Lazy Initialization
```python
# ✅ BON: Lazy initialization avec @property
class MinIOClient:
    def __init__(self, config):
        self.config = config
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization du client S3"""
        if self._client is None:
            self._client = self._create_client()
        return self._client

# Utilisation
client = MinIOClient(config)
s3 = client.client  # Créé à la première utilisation

# ❌ MAUVAIS: Créer partout dans __init__
class MinIOClient:
    def __init__(self, config):
        self.client = boto3.client(...)  # Créé même si pas utilisé
```

### 9. Class Inheritance
```python
# ✅ BON: Héritage clair avec ABC
from abc import ABC, abstractmethod

class ETLComponent(ABC):
    @abstractmethod
    def execute(self) -> bool:
        pass

class Loader(ETLComponent):
    @abstractmethod
    def load(self) -> bool:
        pass
    
    def execute(self) -> bool:
        try:
            self.log_info("Loading...")
            return self.load()
        except Exception as e:
            self.log_error(f"Error: {e}")
            return False

# ❌ MAUVAIS: Pas de contrat clair
class Loader:
    def load(self):
        pass
```

### 10. Context Managers
```python
# ✅ BON: Utiliser context managers pour ressources
from contextlib import contextmanager

@contextmanager
def spark_session():
    spark = SparkSession.builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()

# Utilisation
with spark_session() as spark:
    df = spark.read.csv(...)

# ❌ MAUVAIS: Oublier de fermer les ressources
spark = SparkSession.builder.getOrCreate()
df = spark.read.csv(...)
# Pas de spark.stop()
```

---

## 📋 Code Review Checklist

### Code Style
- [ ] Imports organisés et ordonnés
- [ ] Noms de variables clairs et explicites
- [ ] Pas de lignes > 100 caractères
- [ ] Pas de code commented (le supprimer)
- [ ] Pas de prints() (utiliser logger)

### Fonctionalité
- [ ] Pas de code dupliqué (DRY)
- [ ] Responsabilité unique (SRP)
- [ ] Interfaces claires (ISP)
- [ ] Pas de dépendances cycliques

### Logging & Errors
- [ ] Utilise logger (pas print)
- [ ] Niveaux de log appropriés (DEBUG, INFO, WARNING, ERROR)
- [ ] Error handling spécifique (pas bare except)
- [ ] Messages d'erreur informatifs

### Tests
- [ ] Tous les tests passent
- [ ] Coverage > 80%
- [ ] Docstring pour test cases
- [ ] Pas de tests qui se chevauchent

### Documentation
- [ ] Docstrings complètes
- [ ] README mis à jour
- [ ] Type hints partout
- [ ] Exemples d'utilisation

---

## 🚀 Templates à Copier-Coller

### Template 1: Classe Extracteur
```python
from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger

logger = get_logger(__name__)

class MyExtractor(Extractor):
    """
    Extracteur pour [SOURCE] -> Bronze Layer
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="MyExtractor")
        self.config = config or ConfigManager.get_instance()
    
    def extract(self) -> bool:
        try:
            self.log_info("Extraction starting...")
            
            # TODO: Implémenter la logique d'extraction
            
            self.log_info("✓ Extraction completed")
            return True
        except Exception as e:
            self.log_error(f"Extraction failed: {e}")
            return False
```

### Template 2: Classe Loader
```python
from core.base import Loader
from core.config import ConfigManager
from core.minio_client import MinIOClient
from core.logger import get_logger

logger = get_logger(__name__)

class MyLoader(Loader):
    """
    Loader pour [SOURCE] -> MinIO
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="MyLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            
            # TODO: Implémenter le chargement
            
            self.log_info("✓ Load completed")
            return True
        except Exception as e:
            self.log_error(f"Load failed: {e}")
            return False
```

### Template 3: Classe Transformer
```python
from core.base import Transformer
from core.config import ConfigManager
from core.spark_manager import SparkManager
from core.logger import get_logger
from pyspark.sql import SparkSession

logger = get_logger(__name__)

class MyTransformer(Transformer):
    """
    Transformateur pour [SOURCE] -> Silver Layer
    """
    
    def __init__(self, config: ConfigManager = None):
        super().__init__(name="MyTransformer")
        self.config = config or ConfigManager.get_instance()
        self.spark_manager = SparkManager.get_instance()
    
    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            self.log_info("Transform starting...")
            
            # TODO: Implémenter la transformation
            
            self.log_info("✓ Transform completed")
            return True
        except Exception as e:
            self.log_error(f"Transform failed: {e}")
            return False
```

---

## 📞 Questions Fréquentes

### Q: Dois-je migrer TOUS les fichiers immédiatement?
**A:** Non. Commencez par 1-2 fichiers, testez, puis migrez les autres graduellement.

### Q: Comment tester les nouvelles classes?
**A:** Créez des tests unitaires dans `tests/` utilisant pytest. Voir templates.

### Q: Qu'est-ce que je fais avec les anciens fichiers?
**A:** Après migration et test, supprimez les anciens fichiers procéduraux.

### Q: Dois-je utiliser logging ou print()?
**A:** **TOUJOURS** logging. `print()` n'est jamais en production.

### Q: Comment mettre à jour les DAGs Airflow?
**A:** Remplacez `BashOperator` avec les nouvelles classes:
```python
# ✅ NOUVEAU
from jobs.extract.scrape_books import BookScraper

def run_scraper():
    scraper = BookScraper()
    return scraper.execute()

scrape_task = PythonOperator(
    task_id="scrape",
    python_callable=run_scraper,
)
```

---

## 🎓 Ressources Pédagogiques

- [PEP8 - Style Guide for Python Code](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Official Python Type Hints](https://www.python.org/dev/peps/pep-0484/)
- [Design Patterns in Python](https://en.wikipedia.org/wiki/Design_Patterns)
- [SOLID Principles](https://en.wikipedia.org/wiki/SOLID)
