"""
PHASE ETL : LOAD - Chargement vers MinIO (S3)
Upload des données du bronze-layer vers MinIO avec boto3
"""

import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv(override=False)

# Chemins locaux bronze-layer
BRONZE_SQL_PATH = Path('/opt/airflow/data/bronze-layer/sql')
BRONZE_CSV_PATH = Path('/opt/airflow/data/bronze-layer/csv')
BRONZE_WEB_PATH = Path('/opt/airflow/data/bronze-layer/web')

# Configuration MinIO
MINIO_ENDPOINT =  "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "bronze-layer"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# FONCTIONS HELPERS
# ============================================================

def get_s3_client():
    """
    Créer un client S3 compatible MinIO
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket_exists(s3_client):
    """
    Vérifie que le bucket existe, sinon le crée
    """
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"✓ Bucket '{MINIO_BUCKET}' already exists")
    except ClientError:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"✓ Bucket '{MINIO_BUCKET}' created")


def upload_file(s3_client, local_file, s3_key):
    """
    Upload un fichier vers MinIO
    """
    try:
        file_size = local_file.stat().st_size / 1024  # KB
        
        s3_client.upload_file(
            Filename=str(local_file),
            Bucket=MINIO_BUCKET,
            Key=s3_key
        )
        
        logging.info(f"✓ Uploaded: {s3_key} ({file_size:.2f} KB)")
        return True
        
    except Exception as e:
        logging.error(f"✗ Failed to upload {local_file}: {e}")
        return False


# ============================================================
# LOAD
# ============================================================

def upload_directory(s3_client, base_path, prefix):
    """
    Upload tous les fichiers d'un dossier vers MinIO
    
    Args:
        base_path: Chemin local du dossier
        prefix: Préfixe S3 (ex: 'sql/', 'csv/', 'web/')
    """
    if not base_path.exists():
        logging.warning(f"Path does not exist: {base_path}")
        return 0, 0
    
    uploaded = 0
    total = 0
    
    # Parcourir tous les fichiers Parquet et CSV
    for file_path in base_path.rglob('*'):
        if file_path.is_file() and file_path.suffix in ['.parquet', '.csv']:
            total += 1
            
            # Calculer le chemin S3
            relative_path = file_path.relative_to(base_path)
            s3_key = f"{prefix}{relative_path}"
            
            # Upload
            if upload_file(s3_client, file_path, s3_key):
                uploaded += 1
    
    return uploaded, total


def load_to_minio():
    """
    Upload tous les fichiers du bronze-layer vers MinIO
    """
    try:
        # Créer le client S3
        s3 = get_s3_client()
        logging.info(f"S3 client created: {MINIO_ENDPOINT}")
        
        # Vérifier/créer le bucket
        ensure_bucket_exists(s3)
        
        # Compteurs globaux
        total_uploaded = 0
        total_files = 0
        
        # Upload SQL files
        logging.info("\n📁 Uploading SQL files...")
        uploaded, total = upload_directory(s3, BRONZE_SQL_PATH, 'sql/')
        total_uploaded += uploaded
        total_files += total
        logging.info(f"   SQL: {uploaded}/{total} files")
        
        # Upload CSV files
        logging.info("\n📁 Uploading CSV files...")
        uploaded, total = upload_directory(s3, BRONZE_CSV_PATH, 'csv/')
        total_uploaded += uploaded
        total_files += total
        logging.info(f"   CSV: {uploaded}/{total} files")
        
        # Upload WEB files
        logging.info("\n📁 Uploading WEB files...")
        uploaded, total = upload_directory(s3, BRONZE_WEB_PATH, 'web/')
        total_uploaded += uploaded
        total_files += total
        logging.info(f"   WEB: {uploaded}/{total} files")
        
        # Résumé final
        logging.info("\n" + "=" * 60)
        logging.info(f"✓ LOAD COMPLETED:")
        logging.info(f"  - Files uploaded: {total_uploaded}/{total_files}")
        logging.info(f"  - Bucket: s3://{MINIO_BUCKET}/")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"Load to MinIO failed: {e}")
        raise


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    load_to_minio()