"""Tests avancés pour BooksRawTransformer (PySpark)"""

import pytest
from unittest.mock import MagicMock, patch

from jobs.transform.transform_books import BooksRawTransformer

@patch('jobs.transform.transform_books.SparkSession')
def test_transform_books_success(mock_spark):
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.na.drop.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_spark.read.csv.return_value = mock_df
    mock_minio = MagicMock()
    mock_spark_manager = MagicMock()
    mock_spark_manager.get_session.return_value = mock_spark
    transformer = BooksRawTransformer(
        config=None,
        minio=mock_minio,
        spark_manager=mock_spark_manager
    )
    result = transformer.transform()
    assert result is True

@patch('jobs.transform.transform_books.SparkSession')
def test_transform_books_exception(mock_spark):
    mock_spark.read.csv.side_effect = Exception("fail read")
    mock_minio = MagicMock()
    mock_spark_manager = MagicMock()
    mock_spark_manager.get_session.return_value = mock_spark
    transformer = BooksRawTransformer(
        config=None,
        minio=mock_minio,
        spark_manager=mock_spark_manager
    )
    # Simulate exception in read.csv
    mock_spark.read.csv.side_effect = Exception("fail read")
    result = transformer.transform()
    assert result is False
