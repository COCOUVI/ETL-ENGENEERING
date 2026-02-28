"""
Script de chargement (LOAD) des données RAW vers MinIO (S3 compatible)

Rôle :
- Parcourir les fichiers RAW locaux
- Reproduire la même structure dans MinIO
- Charger les fichiers JSON sans modification

Phase ETL : LOAD
"""

import logging
from pathlib import Path
import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv  

# Charger les variables d'environnement du fichier .env
load_dotenv(override=False)

# =========================
# CONFIGURATION
# =========================

RAW_BASE_PATH= Path("/opt/airflow/data/raw/books")

# Dans jobs/load/load_to_minio.py
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "raw"

# Logging standard industriel
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================
# CONNEXION MINIO
# =========================

def get_s3_client():
    """
    Crée un client S3 compatible MinIO
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

# =========================
# LOAD LOGIC
# =========================

def ensure_bucket_exists(s3_client):
    """
    Vérifie que le bucket existe, sinon le crée
    """
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"Bucket '{MINIO_BUCKET}' already exists")
    except ClientError:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"Bucket '{MINIO_BUCKET}' created")

def upload_raw_files():
    """
    Upload tous les fichiers RAW locaux vers MinIO
    en conservant la structure des dossiers
    """
    s3 = get_s3_client()
    ensure_bucket_exists(s3)

    # Parcours récursif des fichiers JSON
    for file_path in RAW_BASE_PATH.rglob("*.json"):
        # Chemin relatif pour recréer la structure dans MinIO
        relative_path = file_path.relative_to(RAW_BASE_PATH)

        s3_key = f"books/{relative_path}"

        try:
            s3.upload_file(
                Filename=str(file_path),
                Bucket=MINIO_BUCKET,
                Key=s3_key
            )
            logging.info(f"Uploaded {file_path} → s3://{MINIO_BUCKET}/{s3_key}")
        except Exception as e:
            logging.error(f"Failed to upload {file_path}: {e}")

# =========================
# POINT D'ENTRÉE
# =========================

if __name__ == "__main__":
    upload_raw_files()
