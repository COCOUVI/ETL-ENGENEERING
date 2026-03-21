"""Tests avancés pour MinIOClient (couverture maximale)"""

import pytest
from unittest.mock import MagicMock, patch
from core.minio_client import MinIOClient
from core.config import MinIOConfig
from botocore.exceptions import ClientError
from pathlib import Path

@pytest.fixture
def minio_config():
    return MinIOConfig(
        endpoint="http://localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        use_ssl=False
    )

@patch('core.minio_client.boto3.client')
def test_ensure_bucket_exists_creates_bucket(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    error = ClientError({'Error': {'Code': '404'}}, 'HeadBucket')
    mock_s3.head_bucket.side_effect = error
    mock_s3.create_bucket.return_value = None
    client = MinIOClient(minio_config)
    client.ensure_bucket_exists()
    mock_s3.create_bucket.assert_called_once()

@patch('core.minio_client.boto3.client')
def test_ensure_bucket_exists_raises_other_error(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    error = ClientError({'Error': {'Code': '403'}}, 'HeadBucket')
    mock_s3.head_bucket.side_effect = error
    client = MinIOClient(minio_config)
    with pytest.raises(ClientError):
        client.ensure_bucket_exists()

@patch('core.minio_client.boto3.client')
def test_folder_exists_handles_client_error(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.list_objects_v2.side_effect = ClientError({'Error': {'Code': 'NoSuchBucket'}}, 'ListObjectsV2')
    client = MinIOClient(minio_config)
    assert client.folder_exists("test-bucket", "prefix/") is False

@patch('core.minio_client.boto3.client')
def test_upload_file_exception(mock_boto3, minio_config, tmp_path):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    test_file = tmp_path / "fail.csv"
    test_file.write_text("fail")
    mock_s3.upload_file.side_effect = Exception("fail upload")
    client = MinIOClient(minio_config)
    assert client.upload_file(test_file, "fail.csv") is False

@patch('core.minio_client.boto3.client')
def test_upload_directory_not_found(mock_boto3, minio_config, tmp_path):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    client = MinIOClient(minio_config)
    notfound = tmp_path / "notfound"
    uploaded, total = client.upload_directory(notfound, "prefix/")
    assert uploaded == 0 and total == 0

@patch('core.minio_client.boto3.client')
def test_download_file_success(mock_boto3, minio_config, tmp_path):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    out_file = tmp_path / "dl.csv"
    client = MinIOClient(minio_config)
    assert client.download_file("key", out_file) is True

@patch('core.minio_client.boto3.client')
def test_download_file_exception(mock_boto3, minio_config, tmp_path):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    out_file = tmp_path / "dl.csv"
    mock_s3.download_file.side_effect = Exception("fail dl")
    client = MinIOClient(minio_config)
    assert client.download_file("key", out_file) is False

@patch('core.minio_client.boto3.client')
def test_list_objects_exception(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.get_paginator.side_effect = Exception("fail paginator")
    client = MinIOClient(minio_config)
    assert client.list_objects("prefix/") == []

@patch('core.minio_client.boto3.client')
def test_delete_object_exception(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    mock_s3.delete_object.side_effect = Exception("fail delete")
    client = MinIOClient(minio_config)
    assert client.delete_object("key") is False

@patch('core.minio_client.boto3.client')
def test_minio_repr(mock_boto3, minio_config):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    client = MinIOClient(minio_config)
    assert "MinIOClient" in repr(client)
