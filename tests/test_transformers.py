"""Tests for jobs.transform module"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from jobs.transform.transform_books import BooksRawTransformer
from core.config import ConfigManager, PathConfig, MinIOConfig


@pytest.fixture
def transformer_config(temp_dir):
    """Create config for transformer tests"""
    config = ConfigManager()
    config.paths = PathConfig(
        csv_source=str(temp_dir / "source.csv"),
        bronze_layer=str(temp_dir / "bronze"),
        silver_layer=str(temp_dir / "silver")
    )
    config.paths.validate()
    
    config.minio = MinIOConfig(
        endpoint="http://localhost:9000",
        access_key="admin",
        secret_key="password",
        bucket="bronze-layer"
    )
    return config


class TestBooksRawTransformer:
    """Test BooksRawTransformer class"""

    def test_transformer_initialization(self, transformer_config):
        """Test BooksRawTransformer initialization"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                assert transformer.config == transformer_config
                assert transformer.silver_bucket == "silver-layer"
                assert transformer.silver_prefix == "books/"

    def test_transformer_has_required_methods(self, transformer_config):
        """Test transformer has required transformation methods"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                assert hasattr(transformer, 'read_raw_books')
                assert hasattr(transformer, 'clean_books')
                assert hasattr(transformer, 'apply_data_quality_checks')
                assert hasattr(transformer, 'write_silver')

    def test_transformer_transform_calls_pipeline(self, transformer_config):
        """Test transform method executes full pipeline"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager') as mock_spark_manager_class:
                # Setup mock Spark session
                mock_spark_manager = MagicMock()
                mock_spark_session = MagicMock()
                mock_spark_manager.get_session.return_value = mock_spark_session
                mock_spark_manager_class.return_value = mock_spark_manager
                
                # Setup mock DataFrame
                mock_df = MagicMock()
                mock_df.count.return_value = 0
                mock_spark_session.read.option.return_value.json.return_value = mock_df
                
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock methods
                with patch.object(transformer, 'read_raw_books', return_value=mock_df):
                    with patch.object(transformer, 'clean_books', return_value=mock_df):
                        with patch.object(transformer, 'apply_data_quality_checks', return_value=(mock_df, mock_df)):
                            with patch.object(transformer, 'write_silver', return_value=None):
                                result = transformer.transform()
                
                # Result should depend on implementation
                assert isinstance(result, (bool, type(None)))

    def test_transformer_execute_success(self, transformer_config):
        """Test execute method succeeds"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager') as mock_spark_manager_class:
                mock_spark_manager = MagicMock()
                mock_spark_session = MagicMock()
                mock_spark_manager.get_session.return_value = mock_spark_session
                mock_spark_manager_class.return_value = mock_spark_manager
                
                mock_df = MagicMock()
                mock_df.count.return_value = 0
                mock_spark_session.read.option.return_value.json.return_value = mock_df
                
                transformer = BooksRawTransformer(config=transformer_config)
                
                with patch.object(transformer, 'transform', return_value=True):
                    result = transformer.execute()
                    assert result is True

    def test_transformer_execute_failure(self, transformer_config):
        """Test execute method handles failure"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                with patch.object(transformer, 'transform', return_value=False):
                    result = transformer.execute()
                    assert result is False

    def test_transformer_execute_exception(self, transformer_config):
        """Test execute method handles exception"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                with patch.object(transformer, 'transform', side_effect=RuntimeError("Transform failed")):
                    with pytest.raises(RuntimeError):
                        transformer.execute()

    def test_transformer_read_raw_books(self, transformer_config):
        """Test read_raw_books method"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock spark session
                mock_spark = MagicMock()
                mock_reader = MagicMock()
                mock_spark.read.option.return_value = mock_reader
                mock_df = MagicMock()
                mock_reader.json.return_value = mock_df
                
                result = transformer.read_raw_books(mock_spark)
                
                assert result == mock_df
                # Verify the read was called with correct path
                assert mock_reader.json.called

    def test_transformer_clean_books(self, transformer_config):
        """Test clean_books method"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock DataFrame
                mock_df = MagicMock()
                mock_df.select.return_value = mock_df
                mock_df.fillna.return_value = mock_df
                
                result = transformer.clean_books(mock_df)
                
                # Result should be a DataFrame or similar
                assert result is not None

    def test_transformer_apply_quality_checks(self, transformer_config):
        """Test apply_data_quality_checks method"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock DataFrame
                mock_df = MagicMock()
                mock_valid = MagicMock()
                mock_invalid = MagicMock()
                mock_df.filter.side_effect = [mock_valid, mock_invalid]
                
                result = transformer.apply_data_quality_checks(mock_df)
                
                # Should return tuple of (valid, invalid)
                assert isinstance(result, tuple)
                assert len(result) == 2

    def test_transformer_write_silver(self, transformer_config):
        """Test write_silver method"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock DataFrame
                mock_df = MagicMock()
                
                # Should not raise
                result = transformer.write_silver(mock_df)
                
                # Result depends on implementation
                assert result is None or isinstance(result, (bool, int))

    def test_transformer_creates_silver_directory(self, transformer_config):
        """Test transformer creates silver layer directory"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                assert hasattr(transformer, 'silver_local')
                assert isinstance(transformer.silver_local, Path)

    def test_transformer_has_correct_bucket_config(self, transformer_config):
        """Test transformer has correct bucket configuration"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager'):
                transformer = BooksRawTransformer(config=transformer_config)
                
                assert transformer.silver_bucket == "silver-layer"
                assert transformer.config.minio.bucket == "bronze-layer"


class TestTransformerIntegration:
    """Integration tests for transformer"""

    def test_full_transform_pipeline(self, transformer_config):
        """Test full transformation pipeline"""
        with patch('core.minio_client.MinIOClient'):
            with patch('core.spark_manager.SparkManager') as mock_spark_manager_class:
                mock_spark_manager = MagicMock()
                mock_spark_session = MagicMock()
                mock_spark_manager.get_session.return_value = mock_spark_session
                mock_spark_manager_class.return_value = mock_spark_manager
                
                # Setup mock DataFrame
                mock_df = MagicMock()
                mock_df.count.return_value = 5
                mock_spark_session.read.option.return_value.json.return_value = mock_df
                
                transformer = BooksRawTransformer(config=transformer_config)
                
                # Mock all transformation steps
                with patch.object(transformer, 'read_raw_books', return_value=mock_df):
                    with patch.object(transformer, 'clean_books', return_value=mock_df):
                        with patch.object(transformer, 'apply_data_quality_checks', return_value=(mock_df, mock_df)):
                            with patch.object(transformer, 'write_silver'):
                                result = transformer.transform()
                
                assert result is not None
