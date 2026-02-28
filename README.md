## Structure du projet

```
data-engineering-etl/
├── .env
├── .git/
├── .gitignore
├── README.md
├── aws-java-sdk-bundle-1.11.1026.jar
├── config/
│   ├── dev.yaml
│   └── prod.yaml
├── dags/
│   ├── __pycache__/
│   └── books_etl_pipeline.py
├── data/
│   ├── curated/
│   ├── processed/
│   └── raw/
├── docker-compose.yml
├── extractors/
├── hadoop-aws-3.2.4.jar
├── jobs/
│   ├── extract/
│   │   └── scrape_books.py
│   ├── load/
│   │   └── load_to_minio.py
│   └── transform/
│       └── transform_books.py
├── logs/
│   ├── dag_processor_manager/
│   │   └── dag_processor_manager.log
│   └── scheduler/
│       ├── latest
│       └── 2026-02-27/
│           └── books_etl_pipeline.py.log
├── plugins/
├── requirements.txt
├── scripts/
├── spark_jobs/
├── st
├── tests/
├── venv/
```
