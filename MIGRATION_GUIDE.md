# 📋 GUIDE DE MIGRATION - Jobs Restants

## ✅ Jobs déjà refactorisés

- [x] `jobs/extract/extract_csv.py` → `CSVExtractor`
- [x] `jobs/load/load_to_minio.py` → `BooksRawLoader`  
- [x] `jobs/extract/scrape_books.py` → `BookScraper`

## ⏳ Jobs à refactoriser

Suivez ce pattern pour chaque job...

---

## 🎯 PATTERN UNIVERSEL

### Pour un **EXTRACTEUR** (extraction de données)

```python
from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

class MyExtractor(Extractor):
    def __init__(self, config=None):
        super().__init__(name="MyExtractor")
        self.config = config or ConfigManager.get_instance()
    
    def extract(self) -> bool:
        try:
            self.log_info("Extracting...")
            # Votre logique métier
            self.log_info("✓ Done")
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

if __name__ == "__main__":
    setup_logging()
    extractor = MyExtractor()
    import sys
    sys.exit(0 if extractor.execute() else 1)
```

### Pour un **LOADER** (chargement de données)

```python
from core.base import Loader
from core.minio_client import MinIOClient
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

class MyLoader(Loader):
    def __init__(self, config=None):
        super().__init__(name="MyLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            # Votre logique de chargement
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

if __name__ == "__main__":
    setup_logging()
    loader = MyLoader()
    import sys
    sys.exit(0 if loader.execute() else 1)
```

### Pour un **TRANSFORMEUR** (Spark)

```python
from core.base import Transformer
from core.spark_manager import SparkManager
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

class MyTransformer(Transformer):
    def __init__(self, config=None):
        super().__init__(name="MyTransformer")
        self.config = config or ConfigManager.get_instance()
        self.spark_manager = SparkManager.get_instance()
    
    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            # Votre logique Spark
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False

if __name__ == "__main__":
    setup_logging()
    transformer = MyTransformer()
    import sys
    sys.exit(0 if transformer.execute() else 1)
```

---

## 📝 JOBS À MIGRER

### 1️⃣ `jobs/extract/extract_sql.py`

**Type:** Extracteur (PostgreSQL → Parquet)

**Template:**
```python
class SQLExtractor(Extractor):
    def extract(self) -> bool:
        try:
            # Remplacez les hardcoded configs par:
            db_cfg = self.config.database
            
            # Utiliser ConfigManager au lieu de os.getenv()
            conn = psycopg2.connect(
                host=db_cfg.host,
                port=db_cfg.port,
                database=db_cfg.database,
                user=db_cfg.user,
                password=db_cfg.password
            )
            
            # Le reste du code reste pareil
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False
```

**Checklist:**
- [ ] Créer classe héritant de `Extractor`
- [ ] Remplacer `os.getenv()` par `self.config.database`
- [ ] Remplacer `logging.info()` par `self.log_info()`
- [ ] Remplacer `logging.error()` par `self.log_error()`
- [ ] Ajouter `setup_logging()` dans `if __name__`

---

### 2️⃣ `jobs/load/load_csv_to_minio.py`

**Type:** Loader (CSV local → MinIO)

**Template:**
```python
class CSVRawLoader(Loader):
    def __init__(self, config=None):
        super().__init__(name="CSVRawLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            
            # Utiliser le même pattern que BooksRawLoader
            source_dir = Path(self.config.paths.bronze_layer) / "csv"
            uploaded, total = self.minio.upload_directory(
                local_dir=source_dir,
                s3_prefix="csv/",
                extension="*.csv"
            )
            
            return uploaded == total
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False
```

**Note:** C'est presque identique à `BooksRawLoader` - juste changer le préfixe et l'extension!

**Checklist:**
- [ ] Créer classe `CSVRawLoader(Loader)`
- [ ] Copier la structure de `BooksRawLoader`
- [ ] Changer `local_dir` et `s3_prefix` appropriés
- [ ] Changer l'extension de fichiers

---

### 3️⃣ `jobs/load/load_sql_to_minio.py`

**Type:** Loader générique avec boucle (SQL + CSV + WEB → MinIO)

**Template:**
```python
class SQLRawLoader(Loader):
    def __init__(self, config=None):
        super().__init__(name="SQLRawLoader")
        self.config = config or ConfigManager.get_instance()
        self.minio = MinIOClient(self.config.minio)
    
    def load(self) -> bool:
        try:
            self.minio.ensure_bucket_exists()
            
            # Boucle sur les sources
            sources = {
                'sql': Path(self.config.paths.bronze_layer) / 'sql',
                'csv': Path(self.config.paths.bronze_layer) / 'csv',
                'web': Path(self.config.paths.bronze_layer) / 'web',
            }
            
            total_uploaded = 0
            total_files = 0
            
            for prefix, source_path in sources.items():
                if not source_path.exists():
                    self.log_warning(f"Path not found: {source_path}")
                    continue
                
                uploaded, total = self.minio.upload_directory(
                    local_dir=source_path,
                    s3_prefix=f"{prefix}/",
                    extension="*.{parquet,csv}"
                )
                
                total_uploaded += uploaded
                total_files += total
                self.log_info(f"{prefix.upper()}: {uploaded}/{total}")
            
            return total_uploaded == total_files
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False
```

**Checklist:**
- [ ] Créer classe `SQLRawLoader(Loader)`
- [ ] Utiliser boucle `for prefix, source_path in sources.items()`
- [ ] Appeler `minio.upload_directory()` pour chaque source
- [ ] Accumuler les comptes

---

### 4️⃣ `jobs/transform/transform_books.py`

**Type:** Transformeur Spark (JSON → Parquet)

**Clés de Migration:**

```python
# ❌ AVANT
def create_spark_session():
    spark = SparkSession.builder...
    return spark

spark = create_spark_session()

# ✅ APRÈS
spark_manager = SparkManager.get_instance()
spark = spark_manager.get_session()
```

```python
# ❌ AVANT
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9001")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# ✅ APRÈS
minio_cfg = self.config.minio  # Already configured!
```

**Template:**
```python
class BooksTransformer(Transformer):
    def __init__(self, config=None):
        super().__init__(name="BooksTransformer")
        self.config = config or ConfigManager.get_instance()
        self.spark_manager = SparkManager.get_instance()
    
    def transform(self) -> bool:
        try:
            spark = self.spark_manager.get_session()
            
            # Read raw data
            df_raw = self._read_raw_data(spark)
            
            # Clean & transform
            df_clean = self._clean_data(df_raw)
            
            # Write to silver
            self._write_silver_layer(df_clean)
            
            return True
        except Exception as e:
            self.log_error(f"Failed: {e}")
            return False
    
    def _read_raw_data(self, spark):
        path = "s3a://bronze-layer/books/books_*.json"
        return spark.read.option("multiline", "true").json(path)
    
    def _clean_data(self, df):
        # Votre logique de nettoyage
        return df
    
    def _write_silver_layer(self, df):
        output = "s3a://silver-layer/books_enriched"
        df.write.mode("overwrite").parquet(output)
```

**Checklist:**
- [ ] Créer classe `BooksTransformer(Transformer)`
- [ ] Remplacer `create_spark_session()` par `self.spark_manager.get_session()`
- [ ] Remplacer config hardcodée par `self.config.minio`
- [ ] Scinder la logique en méthodes: `_read()`, `_clean()`, `_write()`
- [ ] Remplacer `logging` par `self.log_*()`

---

### 5️⃣ `jobs/transform/transform_csv_books.py`

**Type:** Transformeur Spark (CSV → Parquet)

**Identique à `transform_books.py`** - même pattern!

**Differences:**
```python
# Au lieu de JSON
def _read_raw_data(self, spark):
    path = "s3a://bronze-layer/books/books_*.csv"
    return spark.read.option("header", "true").option("sep", ";").csv(path)
```

**Checklist:** Identique à transform_books.py

---

### 6️⃣ `jobs/transform/transform_sql_books.py`

**Type:** Transformeur Spark (Parquet → Parquet enrichi)

**Pattern identique** aux autres transformeurs!

**Checklist:** Identique à transform_books.py

---

## 🚀 WORKFLOW DE MIGRATION (par job)

### Étape 1: Lire le code ancien
```bash
cat jobs/extract/extract_sql.py
```

### Étape 2: Identifier le type
- **Extract** → Hérite de `Extractor`
- **Load** → Hérite de `Loader`
- **Transform** → Hérite de `Transformer`

### Étape 3: Copier le template approprié
```python
# Utiliser le template correspondant au type
```

### Étape 4: Adapter le code métier
- Logique d'extraction → dans `extract()`
- Logique de chargement → dans `load()`
- Logique de transformation → dans `transform()`

### Étape 5: Remplacer les config
```python
# ❌ AVANT
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

# ✅ APRÈS
minio_cfg = self.config.minio
```

### Étape 6: Remplacer les logs
```python
# ❌ AVANT
logging.info("Message")
logging.error("Error")

# ✅ APRÈS
self.log_info("Message")
self.log_error("Error")
```

### Étape 7: Tester
```bash
cd /path/to/project
python jobs/extract/extract_sql.py
# Devrait voir les logs avec le format standardisé
```

### Étape 8: Mettre à jour le DAG
```python
# jobs/extract/extract_sql.py
from jobs.extract.extract_sql import SQLExtractor

def run_sql_extractor():
    extractor = SQLExtractor()
    return extractor.execute()

# Dans le DAG:
extract_sql = PythonOperator(
    task_id="extract_sql",
    python_callable=run_sql_extractor,
)
```

---

## 📊 PRIORITÉ DE MIGRATION

### 🔴 Crítica (Commencez par là)
1. `extract_sql.py` - Souvent utilisé
2. `load_csv_to_minio.py` - Dupliquation importante
3. `load_sql_to_minio.py` - Dupliquation importante

### 🟡 Important
4. `transform_books.py` - Spark config dupliquée
5. `transform_csv_books.py` - Spark config dupliquée

### 🟢 Nice-to-have
6. `transform_sql_books.py` - Si utilisé

---

## ✅ CHECKLIST COMPLÈTE

```
EXTRACT (3 fichiers)
  ✅ extract_csv.py        [DONE]
  ⏳ extract_sql.py        [TODO - 15 min]
  ✅ scrape_books.py       [DONE]

LOAD (3 fichiers)
  ✅ load_to_minio.py      [DONE]
  ⏳ load_csv_to_minio.py  [TODO - 10 min]
  ⏳ load_sql_to_minio.py  [TODO - 15 min]

TRANSFORM (3 fichiers)
  ⏳ transform_books.py    [TODO - 20 min]
  ⏳ transform_csv_books.py [TODO - 20 min]
  ⏳ transform_sql_books.py [TODO - 20 min]

DAGS (3 fichiers)
  ⏳ books_etl_pipeline.py [TODO - 10 min]
  ⏳ etl_csv_pipeline.py   [TODO - 10 min]
  ⏳ etl_sql_pipeline.py   [TODO - 10 min]

Total: ~2-3h pour tout refactoriser
```

---

## 🎓 COMMANDES UTILES

### Tester un job refactorisé
```bash
python jobs/extract/extract_csv.py
python jobs/load/load_to_minio.py
python jobs/extract/scrape_books.py
```

### Vérifier les imports
```bash
python -c "from core.config import ConfigManager; print(ConfigManager.get_instance())"
```

### Exécuter depuis Airflow (test)
```bash
cd /opt/airflow
python jobs/extract/extract_csv.py
```

---

## 🆘 Questions Fréquentes

### Q: Comment adapter pour d'autres sources?
**A:** Même pattern - change juste la classe de base (`Extractor`/`Loader`/`Transformer`)

### Q: Mon job n'a pas de config?
**A:** Utilise `ConfigManager.get_instance()` - il charge tout

### Q: Je dois passer des paramètres?
**A:** Ajoute à `__init__`:
```python
def __init__(self, source_dir="/data", config=None):
    super().__init__(name="...")
    self.source_dir = source_dir
    self.config = config or ConfigManager.get_instance()
```

### Q: Comment gérer les erreurs?
**A:** Toujours `try/except` et `self.log_error()` + `return False`

### Q: Les tests?
**A:** À faire après - crée un fichier `tests/test_my_job.py`

---

## 📚 Prochains Guides

Après cette migration:
1. Écrire les tests unitaires
2. Mettre à jour les DAGs
3. Documenter les cas d'usage
4. Ajouter monitoring/alerts

---

**Besoin d'aide?** Consultez `BEST_PRACTICES.md` ou `QUICK_START.md`
