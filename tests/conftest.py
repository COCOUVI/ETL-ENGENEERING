import os
import sys
import tempfile
import pytest
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

"""Fixtures and configurations for tests"""

# Create a temporary directory for tests
TEST_TEMP_DIR = tempfile.mkdtemp(prefix="etl_test_")

# Mock pyspark BEFORE any imports from core
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()

# Set environment variables to use temp directories BEFORE config import
os.environ['CSV_SOURCE_PATH'] = os.path.join(TEST_TEMP_DIR, 'source', 'Books.csv')
os.environ['BRONZE_LAYER_PATH'] = os.path.join(TEST_TEMP_DIR, 'bronze-layer')
os.environ['SILVER_LAYER_PATH'] = os.path.join(TEST_TEMP_DIR, 'silver-layer')

from core.config import ConfigManager, MinIOConfig, DatabaseConfig, SparkConfig, PathConfig
from core.minio_client import MinIOClient

# Fixture pour MinIOClient mocké (corrige les erreurs de fixture manquante)
@pytest.fixture
def minio_client(minio_config):
    with patch('core.minio_client.boto3.client') as mock_boto3:
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        client = MinIOClient(minio_config)
        client._client = mock_s3
        yield client


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
