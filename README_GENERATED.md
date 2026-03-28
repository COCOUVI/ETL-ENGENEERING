# Data Engineering ETL Project

## 📌 Information Importante

**Projet de Formation** - Travail autonome réalisé par un seul développeur  
**Destiné à :** Remise au tuteur pour évaluation  
**Statut :** ✅ Complété et testé (couverture >80%)

---

## Structure du projet

```
data-engineering-etl/
├── config/           # Fichiers de configuration (dev.yaml, prod.yaml, utils.py)
├── core/             # Modules principaux (base, config, logger, minio_client, spark_manager...)
├── dags/             # Pipelines Airflow (books_etl_pipeline.py, etl_csv_pipeline.py, etl_sql_pipeline.py)
├── data/             # Données brutes, intermédiaires et sources (bronze-layer, silver-layer, source)
├── extractors/       # Scripts d'extraction de données
├── jobs/             # Scripts ETL (extract, load, transform)
├── plugins/          # Plugins Airflow personnalisés (si utilisés)
├── scripts/          # Scripts utilitaires (ex: books_schema.sql)
├── spark_jobs/       # Jobs Spark spécifiques
├── tests/            # Tests unitaires et d'intégration
├── Dockerfile        # Image Docker du projet
├── docker-compose.yml# Orchestration multi-conteneurs
├── requirements.txt  # Dépendances Python
├── README.md         # Ce fichier
├── INDEX.md, MIGRATION_GUIDE.md, note.txt # Documentation additionnelle
```

### Description des dossiers principaux

- **config/** : Paramètres d'environnement et utilitaires de configuration
- **core/** : Logique métier, gestion Spark, MinIO, logging, etc.
- **dags/** : Définition des DAGs Airflow pour l'orchestration ETL
- **data/** : Données utilisées ou générées par les pipelines (à nettoyer avant soumission)
- **extractors/** : Scripts pour extraire les données sources (CSV, SQL, web...)
- **jobs/** : Scripts ETL découpés par étape (extract, load, transform)
- **plugins/** : Extensions Airflow (optionnel)
- **scripts/** : SQL et autres scripts utilitaires
- **spark_jobs/** : Jobs Spark dédiés
- **tests/** : Couverture de tests >80% (pytest)

> Les dossiers comme `.venv/`, `__pycache__/`, `logs/`, `htmlcov/`, `.pytest_cache/` et les fichiers de données générés doivent être exclus lors de la soumission.

---

## Installation

1. Cloner le dépôt :
   ```bash
   git clone <url-du-repo>
   cd data-engineering-etl
   ```
2. Créer un environnement virtuel et installer les dépendances :
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Configurer les fichiers dans `config/` selon l'environnement (dev/prod).

---

## Exécution des tests

Lancer tous les tests avec couverture :
```bash
pytest tests/ --cov=core --cov=jobs --cov-report=term-missing
```

---

## Fonctionnalités principales

- Extraction de données (CSV, SQL, Web scraping)
- Transformation et nettoyage avec Spark
- Chargement dans MinIO (S3 compatible)
- Orchestration via Airflow (DAGs)
- Tests unitaires et d'intégration (>80% de couverture)

---

## Auteur

Alexandro Cocouvi

---

## Conseils pour la soumission

- Nettoyer les dossiers de logs, caches, environnements virtuels et données générées.
- Vérifier la complétude de la documentation et la clarté du code.
- S'assurer que tous les tests passent et que la couverture reste >80%.
