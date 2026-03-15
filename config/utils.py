import os
import logging
from dotenv import load_dotenv

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config():
    load_dotenv(override=False)
    config = {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
        "SOURCE_DB_HOST": os.getenv("SOURCE_DB_HOST", "postgres-source"),
        "SOURCE_DB_PORT": os.getenv("SOURCE_DB_PORT", "5432"),
        "SOURCE_DB_NAME": os.getenv("SOURCE_DB_NAME", "sourcedb"),
        "SOURCE_DB_USER": os.getenv("SOURCE_DB_USER", "sourceuser"),
        "SOURCE_DB_PASSWORD": os.getenv("SOURCE_DB_PASSWORD", "sourcepass"),
    }
    return config
