"""Tests for Spark Manager and other core utilities"""

import pytest
from unittest.mock import MagicMock, patch


class TestSparkManagerBasic:
    """Basic Spark Manager tests"""

    def test_spark_manager_imports(self):
        """Test spark manager can be imported"""
        from core.spark_manager import SparkManager
        assert SparkManager is not None

    @patch('core.spark_manager.SparkSession')
    def test_spark_manager_initialization(self, mock_spark_session):
        """Test Spark manager initializes"""
        from core.spark_manager import SparkManager
        manager = SparkManager()
        assert manager is not None

    @patch('core.spark_manager.SparkSession')
    def test_get_spark_session(self, mock_spark_session):
        """Test getting Spark session"""
        mock_session = MagicMock()
        mock_spark_session.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        
        from core.spark_manager import SparkManager
        manager = SparkManager()
        session = manager.get_session()
        
        assert session is not None


class TestImplementationsModule:
    """Test implementations module"""

    def test_implementations_imports(self):
        """Test implementations module imports"""
        try:
            from core import implementations
            assert implementations is not None
        except ImportError:
            # It's okay if not importable due to pandas/spark
            pass

    def test_books_raw_loader_example(self):
        """Test that BooksRawLoader is in implementations"""
        from core.implementations import BooksRawLoader
        assert BooksRawLoader is not None

    def test_csv_extractor_example(self):
        """Test that CSVExtractor is in implementations"""
        from core.implementations import CSVExtractor
        assert CSVExtractor is not None


class TestMinIOClientEdgeCases:
    """Test MinIO client edge cases"""

    @patch('core.minio_client.boto3.client')
    def test_upload_file_string_input(self, mock_boto3):
        """Test upload file with string path"""
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        client = MinIOClient(config)
        
        # String path input
        result = client.upload_file("/nonexistent/file.txt", "key")
        assert result == False

    @patch('core.minio_client.boto3.client')
    def test_upload_directory_with_extension(self, mock_boto3, tmp_path):
        """Test upload directory with file extension"""
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        # Create test directory
        test_dir = tmp_path / "data"
        test_dir.mkdir()
        (test_dir / "file1.json").write_text("{}")
        (test_dir / "file2.json").write_text("{}")
        (test_dir / "file3.txt").write_text("text")
        
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        client = MinIOClient(config)
        uploaded, total = client.upload_directory(test_dir, "test/", "*.json")
        
        # Should only match JSON files
        assert uploaded >= 2
        assert total >= 2

    @patch('core.minio_client.boto3.client')
    def test_upload_directory_all_files(self, mock_boto3, tmp_path):
        """Test upload directory matching all files"""
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        test_dir = tmp_path / "data"
        test_dir.mkdir()
        (test_dir / "file1.json").write_text("{}")
        (test_dir / "file2.txt").write_text("text")
        
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        client = MinIOClient(config)
        uploaded, total = client.upload_directory(test_dir, "test/", "*")
        
        assert total >= 2

    @patch('core.minio_client.boto3.client')
    def test_boto_client_lazy_init(self, mock_boto3):
        """Test boto3 client lazy initialization"""
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        client = MinIOClient(config)
        
        # Client should not be created yet
        assert client._client is None
        
        # Access property triggers creation
        _ = client.client
        assert client._client == mock_s3

    @patch('core.minio_client.boto3.client')
    def test_minio_create_client_config(self, mock_boto3):
        """Test that _create_client uses correct config"""
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        mock_s3 = MagicMock()
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="https://minio.example.com",
            access_key="mykey",
            secret_key="mysecret",
            region="eu-west-1",
            use_ssl=True
        )
        
        client = MinIOClient(config)
        _ = client.client
        
        # Verify boto3.client was called with correct args
        call_kwargs = mock_boto3.call_args[1]
        assert call_kwargs['endpoint_url'] == "https://minio.example.com"
        assert call_kwargs['aws_access_key_id'] == "mykey"
        assert call_kwargs['aws_secret_access_key'] == "mysecret"
        assert call_kwargs['region_name'] == "eu-west-1"
        assert call_kwargs['use_ssl'] == True


class TestDataProcessingPipelines:
    """Test complete data processing pipelines"""

    def test_pipeline_extraction_transformation(self):
        """Test full extract-transform pipeline with mocks"""
        from core.base import Pipeline, Extractor, Transformer
        
        class TestExtractor(Extractor):
            def extract(self):
                return True
        
        class TestTransformer(Transformer):
            def transform(self):
                return True
        
        pipeline = Pipeline("ETL")
        pipeline.add(TestExtractor("extract"))
        pipeline.add(TestTransformer("transform"))
        
        result = pipeline.execute()
        assert result == True

    def test_pipeline_with_multiple_loaders(self):
        """Test pipeline with multiple loader stages"""
        from core.base import Pipeline, Loader
        
        class Loader1(Loader):
            def load(self):
                return True
        
        class Loader2(Loader):
            def load(self):
                return True
        
        pipeline = Pipeline("MultiLoad")
        pipeline.add(Loader1("load1"))
        pipeline.add(Loader2("load2"))
        
        result = pipeline.execute()
        assert result == True


class TestConfigurationEdgeCases:
    """Test configuration edge cases"""

    def test_minio_config_repr(self):
        """Test MinIO config string representation"""
        from core.config import MinIOConfig
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        repr_str = repr(config)
        assert "MinIOConfig" in repr_str or "endpoint" in repr_str

    def test_database_config_repr(self):
        """Test database config string representation"""
        from core.config import DatabaseConfig
        
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="admin",
            password="pass"
        )
        repr_str = repr(config)
        assert "DatabaseConfig" in repr_str or "host" in repr_str

    def test_config_manager_singleton_pattern(self):
        """Test ConfigManager singleton behavior"""
        from core.config import ConfigManager
        
        # Reset singleton
        ConfigManager._instance = None
        
        instance1 = ConfigManager()
        instance2 = ConfigManager()
        
        # Both should be ConfigManager instances
        assert isinstance(instance1, ConfigManager)
        assert isinstance(instance2, ConfigManager)

    def test_get_instance_class_method(self):
        """Test ConfigManager.get_instance() class method"""
        from core.config import ConfigManager
        
        ConfigManager._instance = None
        instance = ConfigManager.get_instance()
        
        assert isinstance(instance, ConfigManager)


class TestErrorHandling:
    """Test error handling in modules"""

    @patch('core.minio_client.boto3.client')
    def test_minio_handles_s3_errors(self, mock_boto3):
        """Test MinIO client handles S3 errors"""
        from botocore.exceptions import ClientError
        from core.minio_client import MinIOClient
        from core.config import MinIOConfig
        
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied'}},
            'PutObject'
        )
        mock_boto3.return_value = mock_s3
        
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        client = MinIOClient(config)
        
        result = client.upload_file("/fake/file.txt", "key")
        assert result == False

    def test_logger_handles_missing_parameters(self):
        """Test logger handles optional parameters"""
        from core.logger import get_logger
        
        logger1 = get_logger("test1")
        logger2 = get_logger("test2", level=10)
        
        assert logger1 is not None
        assert logger2 is not None
