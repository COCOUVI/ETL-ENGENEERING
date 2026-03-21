
import pytest
import logging
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

"""Fixtures and configurations for tests"""

# Fixture pour MinIOClient mocké (corrige les erreurs de fixture manquante)
@pytest.fixture
def minio_client(minio_config):
    with patch('core.minio_client.boto3.client') as mock_boto3:
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        client = MinIOClient(minio_config)
        client._client = mock_s3
        yield client

# Mock pyspark before importing core modules
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

# Mock os.makedirs to prevent permission issues during config loading
original_makedirs = os.makedirs
def mock_makedirs(*args, **kwargs):
    """Mock makedirs that doesn't fail on /opt/airflow"""
    path = args[0] if args else None
    if path and '/opt/airflow' in str(path):
        return  # Skip actual creation
    return original_makedirs(*args, **kwargs)

os.makedirs = mock_makedirs

from core.config import ConfigManager, MinIOConfig, DatabaseConfig, SparkConfig, PathConfig


@pytest.fixture
def temp_dir():
    """Create temporary directory for tests"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_minio_config():
    """Create mock MinIO configuration"""
    return MinIOConfig(
        endpoint="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        region="us-east-1",
        use_ssl=False
    )


@pytest.fixture
def mock_db_config():
    """Create mock database configuration"""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        database="testdb",
        user="testuser",
        password="testpass"
    )


@pytest.fixture
def mock_spark_config():
    """Create mock Spark configuration"""
    return SparkConfig(
        app_name="ETL-Test",
        shuffle_partitions=4,
        log_level="OFF"
    )


@pytest.fixture
def mock_path_config(temp_dir):
    """Create mock path configuration"""
    return PathConfig(
        csv_source=str(temp_dir / "source.csv"),
        bronze_layer=str(temp_dir / "bronze"),
        silver_layer=str(temp_dir / "silver")
    )


@pytest.fixture
def mock_config_manager(mock_minio_config, mock_db_config, mock_spark_config, mock_path_config):
    """Create mock ConfigManager"""
    with patch.dict('os.environ', {
        'MINIO_ENDPOINT': mock_minio_config.endpoint,
        'MINIO_ROOT_USER': mock_minio_config.access_key,
        'MINIO_ROOT_PASSWORD': mock_minio_config.secret_key,
        'MINIO_BUCKET': mock_minio_config.bucket,
        'SOURCE_DB_HOST': mock_db_config.host,
        'SOURCE_DB_PORT': str(mock_db_config.port),
        'SOURCE_DB_NAME': mock_db_config.database,
        'SOURCE_DB_USER': mock_db_config.user,
        'SOURCE_DB_PASSWORD': mock_db_config.password,
    }):
        # Reset singleton
        ConfigManager._instance = None
        config = ConfigManager()
        config.minio = mock_minio_config
        config.database = mock_db_config
        config.spark = mock_spark_config
        config.paths = mock_path_config
        return config


@pytest.fixture
def caplog_debug():
    """Capture logs at DEBUG level"""
    with patch.object(logging.getLogger(), 'setLevel', return_value=None):
        yield
