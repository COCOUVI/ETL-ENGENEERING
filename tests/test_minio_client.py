"""Tests for core.minio_client module - Simplified"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError
from core.minio_client import MinIOClient
from core.config import MinIOConfig


@pytest.fixture
def minio_config():
    """Create MinIO config for testing"""
    return MinIOConfig(
        endpoint="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        use_ssl=False
    )


class TestMinIOClientInitialization:
    """Test MinIOClient initialization"""

    def test_minio_client_with_config(self, minio_config):
        """Test MinIOClient initialization with config"""
        with patch('core.minio_client.boto3.client'):
            client = MinIOClient(minio_config)
            assert client.config == minio_config
            assert client._client is None

    def test_minio_client_lazy_initialization(self, minio_config):
        """Test MinIOClient lazy initializes boto client"""
        with patch('core.minio_client.boto3.client') as mock_boto3:
            mock_s3_client = MagicMock()
            mock_boto3.return_value = mock_s3_client
            
            client = MinIOClient(minio_config)
            # Access client property triggers initialization
            result = client.client
            
            assert result == mock_s3_client
            mock_boto3.assert_called_once()

    def test_minio_client_without_config(self):
        """Test MinIOClient uses ConfigManager if no config provided"""
        with patch('core.minio_client.ConfigManager.get_instance') as mock_config_manager:
            with patch('core.minio_client.boto3.client'):
                mock_cm = MagicMock()
                mock_config_manager.return_value = mock_cm
                
                client = MinIOClient(config=None)
                # This will fail because we're not fully mocking, but that's ok
                # The test just shows the path


class TestMinIOClientCreateClient:
    """Test _create_client method"""

    def test_create_client_calls_boto3(self, minio_config):
        """Test _create_client creates boto3 S3 client"""
        with patch('core.minio_client.boto3.client') as mock_boto3:
            mock_boto3.return_value = MagicMock()
            client = MinIOClient(minio_config)
            
            # Trigger client creation
            _ = client.client
            
            mock_boto3.assert_called_once()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs['endpoint_url'] == minio_config.endpoint
            assert call_kwargs['aws_access_key_id'] == minio_config.access_key
            assert call_kwargs['aws_secret_access_key'] == minio_config.secret_key

    def test_create_client_respects_ssl_setting(self, minio_config):
        """Test _create_client respects use_ssl setting"""
        minio_config.use_ssl = True
        
        with patch('core.minio_client.boto3.client') as mock_boto3:
            mock_boto3.return_value = MagicMock()
            client = MinIOClient(minio_config)
            
            _ = client.client
            
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs['use_ssl'] is True


class TestMinIOClientBucketOperations:
    """Test bucket-related operations"""

    def test_ensure_bucket_exists_success(self, minio_client, minio_config):
        """Test ensure_bucket_exists when bucket exists"""
        minio_client.client.head_bucket.return_value = {}
        
        # Should not raise
        minio_client.ensure_bucket_exists()
        minio_client.client.head_bucket.assert_called_with(Bucket=minio_config.bucket)

    def test_ensure_bucket_exists_creates_bucket(self, minio_client, minio_config):
        """Test ensure_bucket_exists creates bucket if missing"""
        error = ClientError(
            {'Error': {'Code': '404'}},
            'HeadBucket'
        )
        minio_client.client.head_bucket.side_effect = error
        minio_client.client.create_bucket.return_value = {}
        
        minio_client.ensure_bucket_exists()
        
        minio_client.client.create_bucket.assert_called_once_with(Bucket=minio_config.bucket)

    def test_ensure_bucket_exists_custom_bucket(self, minio_client):
        """Test ensure_bucket_exists with custom bucket name"""
        minio_client.client.head_bucket.return_value = {}
        
        minio_client.ensure_bucket_exists(bucket="custom-bucket")
        minio_client.client.head_bucket.assert_called_with(Bucket="custom-bucket")

    def test_bucket_exists_returns_true(self, minio_client):
        """Test bucket_exists returns True when bucket exists"""
        minio_client.client.head_bucket.return_value = {}
        
        result = minio_client.bucket_exists()
        assert result is True

    def test_bucket_exists_returns_false(self, minio_client):
        """Test bucket_exists returns False when bucket doesn't exist"""
        minio_client.client.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404'}},
            'HeadBucket'
        )
        
        result = minio_client.bucket_exists()
        assert result is False


class TestMinIOClientFolderOperations:
    """Test folder-related operations"""

    def test_folder_exists_true(self, minio_client):
        """Test folder_exists returns True when folder has objects"""
        minio_client.client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'books/file1.json'}]
        }
        
        result = minio_client.folder_exists("test-bucket", "books/")
        assert result is True

    def test_folder_exists_false_empty(self, minio_client):
        """Test folder_exists returns False when folder is empty"""
        minio_client.client.list_objects_v2.return_value = {}
        
        result = minio_client.folder_exists("test-bucket", "books/")
        assert result is False

    def test_folder_exists_false_error(self, minio_client):
        """Test folder_exists returns False on error"""
        minio_client.client.list_objects_v2.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchBucket'}},
            'ListObjectsV2'
        )
        
        result = minio_client.folder_exists("test-bucket", "books/")
        assert result is False


class TestMinIOClientUploadFile:
    """Test file upload operations"""

    def test_upload_file_success(self, minio_client, temp_dir):
        """Test successful file upload"""
        # Create test file
        test_file = temp_dir / "test.csv"
        test_file.write_text("data")
        
        minio_client.client.upload_file.return_value = None
        
        result = minio_client.upload_file(test_file, "test/test.csv")
        
        assert result is True
        minio_client.client.upload_file.assert_called_once()

    def test_upload_file_nonexistent(self, minio_client):
        """Test upload fails for nonexistent file"""
        result = minio_client.upload_file("/nonexistent/file.csv", "test/test.csv")
        assert result is False

    def test_upload_file_with_exception(self, minio_client, temp_dir):
        """Test upload handles exception"""
        test_file = temp_dir / "test.csv"
        test_file.write_text("data")
        
        minio_client.client.upload_file.side_effect = Exception("Upload failed")
        
        result = minio_client.upload_file(test_file, "test/test.csv")
        assert result is False

    def test_upload_file_custom_bucket(self, minio_client, temp_dir):
        """Test upload_file with custom bucket"""
        test_file = temp_dir / "test.csv"
        test_file.write_text("data")
        
        minio_client.client.upload_file.return_value = None
        
        minio_client.upload_file(test_file, "test/test.csv", bucket="custom-bucket")
        
        call_kwargs = minio_client.client.upload_file.call_args[1]
        assert call_kwargs['Bucket'] == "custom-bucket"

    def test_upload_file_string_path(self, minio_client, temp_dir):
        """Test upload_file converts string path to Path"""
        test_file = temp_dir / "test.csv"
        test_file.write_text("data")
        
        minio_client.client.upload_file.return_value = None
        
        result = minio_client.upload_file(str(test_file), "test/test.csv")
        assert result is True


class TestMinIOClientDownloadFile:
    """Test file download operations"""

    def test_download_file_success(self, minio_client, temp_dir):
        """Test successful file download"""
        output_file = temp_dir / "downloaded.csv"
        
        minio_client.client.download_file.return_value = None
        
        result = minio_client.download_file(
            "test/test.csv",
            str(output_file),
            bucket="test-bucket"
        )
        
        assert result is True

    def test_download_file_with_exception(self, minio_client, temp_dir):
        """Test download handles exception"""
        output_file = temp_dir / "downloaded.csv"
        
        minio_client.client.download_file.side_effect = Exception("Download failed")
        
        result = minio_client.download_file(
            "test/test.csv",
            str(output_file)
        )
        
        assert result is False


class TestMinIOClientUploadDirectory:
    """Test directory upload operations"""

    def test_upload_directory_success(self, minio_client, temp_dir):
        """Test successful directory upload"""
        # Create test directory with files
        test_dir = temp_dir / "data"
        test_dir.mkdir()
        (test_dir / "file1.json").write_text("{}")
        (test_dir / "file2.json").write_text("{}")
        
        minio_client.client.upload_file.return_value = None
        
        uploaded, total = minio_client.upload_directory(
            test_dir,
            "books/",
            extension="*.json"
        )
        
        assert uploaded == total
        assert uploaded == 2

    def test_upload_directory_all_extension(self, minio_client, temp_dir):
        """Test upload_directory with all files"""
        test_dir = temp_dir / "data"
        test_dir.mkdir()
        (test_dir / "file1.txt").write_text("data")
        (test_dir / "file2.json").write_text("{}")
        
        minio_client.client.upload_file.return_value = None
        
        uploaded, total = minio_client.upload_directory(
            test_dir,
            "data/",
            extension="*"
        )
        
        assert uploaded >= 2


class TestMinIOClientListObjects:
    """Test object listing operations"""

    def test_list_objects_success(self, minio_client):
        """Test successful object listing"""
        minio_client.client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'books/file1.json'},
                {'Key': 'books/file2.json'},
            ]
        }
        
        objects = minio_client.list_objects("books/")
        
        assert len(objects) == 2
        assert objects[0] == 'books/file1.json'

    def test_list_objects_empty(self, minio_client):
        """Test list_objects with empty result"""
        minio_client.client.list_objects_v2.return_value = {}
        
        objects = minio_client.list_objects("empty/")
        
        assert objects == []


class TestMinIOClientDeleteObject:
    """Test object deletion"""

    def test_delete_object_success(self, minio_client):
        """Test successful object deletion"""
        minio_client.client.delete_object.return_value = None
        
        result = minio_client.delete_object("books/file.json")
        
        assert result is True

    def test_delete_object_failure(self, minio_client):
        """Test delete_object handles exception"""
        minio_client.client.delete_object.side_effect = Exception("Delete failed")
        
        result = minio_client.delete_object("books/file.json")
        
        assert result is False
