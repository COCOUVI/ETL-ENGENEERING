"""Tests for jobs.load module"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from jobs.load.load_csv_to_minio import BooksRawCSVLoader
from core.config import ConfigManager, PathConfig, MinIOConfig


@pytest.fixture
def loader_config(temp_dir):
    """Create config for loader tests"""
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
        bucket="test-bucket"
    )
    return config


class TestBooksRawCSVLoader:
    """Test BooksRawCSVLoader class"""

    def test_loader_initialization(self, loader_config):
        """Test BooksRawCSVLoader initialization"""
        with patch('core.minio_client.MinIOClient'):
            loader = BooksRawCSVLoader(config=loader_config)
            assert loader.config == loader_config
            assert loader.raw_path == Path(loader_config.paths.bronze_layer) / "books"

    def test_loader_no_files_found(self, loader_config):
        """Test loader returns False when no CSV files found"""
        with patch('core.minio_client.MinIOClient') as mock_minio_class:
            mock_minio = MagicMock()
            mock_minio_class.return_value = mock_minio
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.load()
            
            assert result is False

    def test_loader_successful_upload(self, temp_dir, loader_config):
        """Test successful CSV upload to MinIO"""
        # Create test CSV file
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv_file = bronze_dir / "test.csv"
        csv_file.write_text("title,author\nBook1,Author1\n")
        
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            # Mock the boto3 S3 client
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            mock_s3_client.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
            mock_s3_client.upload_file.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.load()
            
            assert result is True
            mock_s3_client.head_bucket.assert_called()
            mock_s3_client.upload_file.assert_called()

    def test_loader_upload_exception(self, temp_dir, loader_config):
        """Test loader handles upload exception"""
        # Create test CSV file
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv_file = bronze_dir / "test.csv"
        csv_file.write_text("data")
        
        with patch('core.minio_client.MinIOClient') as mock_minio_class:
            mock_minio = MagicMock()
            mock_minio_class.return_value = mock_minio
            mock_minio.ensure_bucket_exists.return_value = None
            mock_minio.upload_file.side_effect = Exception("Upload error")
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.load()
            
            assert result is False

    def test_loader_execute_calls_load(self, temp_dir, loader_config):
        """Test execute method calls load"""
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv_file = bronze_dir / "test.csv"
        csv_file.write_text("data")
        
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            mock_s3_client.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
            mock_s3_client.upload_file.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.execute()
            
            assert result is True

    def test_loader_multiple_csv_files(self, temp_dir, loader_config):
        """Test loader finds first CSV file"""
        # Create multiple CSV files
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv1 = bronze_dir / "file1.csv"
        csv1.write_text("data1")
        csv2 = bronze_dir / "file2.csv"
        csv2.write_text("data2")
        
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            mock_s3_client.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
            mock_s3_client.upload_file.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.load()
            
            assert result is True
            # Should upload one of the files
            mock_s3_client.upload_file.assert_called_once()

    def test_loader_ignores_non_csv_files(self, temp_dir, loader_config):
        """Test loader ignores non-CSV files"""
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        txt_file = bronze_dir / "test.txt"
        txt_file.write_text("not csv")
        
        with patch('core.minio_client.MinIOClient') as mock_minio_class:
            mock_minio = MagicMock()
            mock_minio_class.return_value = mock_minio
            mock_minio.ensure_bucket_exists.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            result = loader.load()
            
            assert result is False

    def test_loader_uses_correct_s3_key(self, temp_dir, loader_config):
        """Test loader uses correct S3 key format"""
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv_file = bronze_dir / "test.csv"
        csv_file.write_text("data")
        
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            mock_s3_client.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
            mock_s3_client.upload_file.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            loader.load()
            
            # Check S3 key format
            call_kwargs = mock_s3_client.upload_file.call_args[1]
            assert call_kwargs['Key'].startswith("books/")

    def test_loader_creates_correct_bucket_config(self, loader_config):
        """Test loader initializes with correct config"""
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            
            loader = BooksRawCSVLoader(config=loader_config)
            
            # Verify loader has correct config
            assert loader.config == loader_config
            assert loader.bucket_name == loader_config.minio.bucket


class TestCSVLoaderIntegration:
    """Integration tests for CSV loader"""

    def test_full_load_pipeline(self, temp_dir, loader_config):
        """Test full load pipeline"""
        # Setup source CSV
        bronze_dir = temp_dir / "bronze" / "books"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        csv_file = bronze_dir / "books_2024-01-01.csv"
        csv_file.write_text("title,author\nBook1,Author1\n")
        
        with patch('core.minio_client.boto3.client') as mock_boto3_client:
            mock_s3_client = MagicMock()
            mock_boto3_client.return_value = mock_s3_client
            mock_s3_client.head_bucket.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
            mock_s3_client.upload_file.return_value = None
            
            loader = BooksRawCSVLoader(config=loader_config)
            success = loader.execute()
            
            assert success is True
            mock_s3_client.head_bucket.assert_called()
            mock_s3_client.upload_file.assert_called()

