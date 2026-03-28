"""Tests avancés pour SparkManager (couverture maximale)"""

import pytest
from unittest.mock import patch, MagicMock
from core.spark_manager import SparkManager
from core.config import SparkConfig

@pytest.fixture
def spark_config():
    return SparkConfig(app_name="TestApp", shuffle_partitions=2, log_level="WARN")

@patch('core.spark_manager.SparkSession.builder')
def test_get_or_create_session_success(mock_builder, spark_config):
    # Reset the singleton
    SparkManager._session = None
    
    # Setup the mock builder chain
    mock_session = MagicMock()
    mock_session.sparkContext.master = "local"
    mock_session.sparkContext.appName = "TestApp"
    
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session
    
    mock_config = MagicMock()
    mock_config.spark = spark_config
    mock_config.minio.endpoint = "http://localhost:9000"
    mock_config.minio.access_key = "test"
    mock_config.minio.secret_key = "test"
    
    manager = SparkManager(spark_config, mock_config.minio)
    session = manager.get_or_create_session()
    assert session is not None
    
    # Reset for next test
    SparkManager._session = None

@patch('core.spark_manager.SparkSession.builder')
def test_stop_session(mock_builder, spark_config):
    # Reset the singleton
    SparkManager._session = None
    
    # Setup the mock builder chain
    mock_session = MagicMock()
    mock_session.sparkContext.master = "local"
    mock_session.sparkContext.appName = "TestApp"
    
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session
    
    mock_config = MagicMock()
    mock_config.spark = spark_config
    mock_config.minio.endpoint = "http://localhost:9000"
    mock_config.minio.access_key = "test"
    mock_config.minio.secret_key = "test"
    
    manager = SparkManager(spark_config, mock_config.minio)
    session = manager.get_or_create_session()
    assert session is not None
    
    # Stop session
    manager.stop_session()
    assert SparkManager._session is None
    
    # Can be called again safely
    manager.stop_session()
    
    # Reset for next test
    SparkManager._session = None

@patch('core.spark_manager.SparkSession')
def test_repr(mock_spark, spark_config):
    manager = SparkManager(spark_config)
    assert "SparkManager" in repr(manager)
