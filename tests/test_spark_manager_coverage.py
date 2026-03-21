"""Tests avancés pour SparkManager (couverture maximale)"""

import pytest
from unittest.mock import patch, MagicMock
from core.spark_manager import SparkManager
from core.config import SparkConfig

@pytest.fixture
def spark_config():
    return SparkConfig(app_name="TestApp", shuffle_partitions=2, log_level="WARN")

@patch('core.spark_manager.SparkSession')
def test_get_or_create_session_success(mock_spark, spark_config):
    mock_builder = MagicMock()
    mock_spark.builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = "session"
    manager = SparkManager(spark_config)
    session = manager.get_or_create_session()
    assert session == "session"
    # Appel à nouveau pour le cache
    session2 = manager.get_or_create_session()
    assert session2 == "session"

@patch('core.spark_manager.SparkSession')
def test_stop_session(mock_spark, spark_config):
    mock_builder = MagicMock()
    mock_spark.builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock(stop=MagicMock())
    manager = SparkManager(spark_config)
    manager.get_or_create_session()
    manager.stop_session()
    # Peut être appelé même si déjà None
    manager.stop_session()

@patch('core.spark_manager.SparkSession')
def test_repr(mock_spark, spark_config):
    manager = SparkManager(spark_config)
    assert "SparkManager" in repr(manager)
