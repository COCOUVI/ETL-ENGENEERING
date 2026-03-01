import logging

from pathlib import Path

import boto3

from  dotenv import load_dotenv


load_dotenv(override=False)


RAW_PATH = Path('/opt/airflow/data/bronze-layer/books/')


# Configuration 

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "bronze-layer"

logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

def get_S3_client():
    """
      Creer un CLIENT S3 COMPATIBLE 
    """
    return boto3.Client(
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
        logging.info(f"Bucket '{MINIO_BUCKET}' already exists")
    except ClientError:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"Bucket '{MINIO_BUCKET}' created")


def upload_raw_files():
    """
    Upload tous les fichiers RAW locaux vers MinIO
    en conservant la structure des dossiers
    """
    s3 = get_S3_client()
    ensure_bucket_exists(s3)
    # relative_path = fi    





