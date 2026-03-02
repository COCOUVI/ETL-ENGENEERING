import logging

from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from  dotenv import load_dotenv
import os


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
        logging.info(f"Bucket '{MINIO_BUCKET}' already exists")
    except ClientError:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"Bucket '{MINIO_BUCKET}' created")


def upload_raw_file():
    """
    Upload un seul fichier RAW vers MinIO
    """
    s3 = get_S3_client()
    ensure_bucket_exists(s3)

    # trouver le fichier sans connaître son nom
    file_path = next(RAW_PATH.rglob("*.csv"), None)

    if not file_path:
        logging.warning("Aucun fichier CSV trouvé dans RAW_PATH")
        return

    relative_path = file_path.relative_to(RAW_PATH)
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


if __name__ == "__main__":
    upload_raw_file()