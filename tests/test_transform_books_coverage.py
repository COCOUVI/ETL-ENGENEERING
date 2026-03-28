"""Tests avancés pour BooksRawTransformer (PySpark)"""

import pytest
from unittest.mock import MagicMock, patch

from jobs.transform.transform_books import BooksRawTransformer

def test_transform_books_success():
    mock_df = MagicMock()
    mock_df.show.return_value = None
    mock_df.printSchema.return_value = None
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.na.drop.return_value = mock_df
    mock_df.na.fill.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.count.return_value = 10
    
    mock_minio = MagicMock()
    mock_minio.ensure_bucket_exists.return_value = None
    mock_minio.upload_file.return_value = True
    
    mock_spark = MagicMock()
    mock_spark.read.json.return_value = mock_df
    
    mock_spark_manager = MagicMock()
    mock_spark_manager.get_session.return_value = mock_spark
    
    transformer = BooksRawTransformer(config=None)
    transformer.minio = mock_minio
    transformer.spark_manager = mock_spark_manager
    
    # Mock all internal methods to return what transform() expects
    transformer.read_raw_books = MagicMock(return_value=mock_df)
    transformer.clean_books = MagicMock(return_value=mock_df)
    transformer.apply_data_quality_checks = MagicMock(return_value=(mock_df, mock_df))
    transformer.write_silver = MagicMock(return_value=None)
    
    result = transformer.transform()
    assert result is True

def test_transform_books_exception():
    mock_spark_manager = MagicMock()
    mock_spark_manager.get_session.side_effect = Exception("fail read")
    
    transformer = BooksRawTransformer(config=None)
    transformer.spark_manager = mock_spark_manager
    
    result = transformer.transform()
    assert result is False
