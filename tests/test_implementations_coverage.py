"""Tests avancés pour core.implementations (Pipeline, Loader, Extractor, Transformer)"""

import pytest
from core.implementations import (
    BooksRawLoader, BooksTransformer, CSVExtractor
)
from core.base import Pipeline, Extractor, Transformer, Loader
from unittest.mock import MagicMock, patch

class DummyExtractor(Extractor):
    def __init__(self):
        super().__init__(name="DummyExtractor")
    def extract(self):
        return True

class DummyTransformer(Transformer):
    def __init__(self):
        super().__init__(name="DummyTransformer")
    def transform(self):
        return True

class DummyLoader(Loader):
    def __init__(self):
        super().__init__(name="DummyLoader")
    def load(self):
        return True

def test_pipeline_run_success():
    pipeline = Pipeline("Test Pipeline")
    pipeline.add(DummyExtractor())
    pipeline.add(DummyTransformer())
    pipeline.add(DummyLoader())
    result = pipeline.run()
    assert result is True

def test_pipeline_run_exception():
    class BadExtractor(Extractor):
        def __init__(self):
            super().__init__(name="BadExtractor")
        def extract(self):
            raise Exception("fail extract")
    pipeline = Pipeline("Test Pipeline")
    pipeline.add(BadExtractor())
    pipeline.add(DummyTransformer())
    pipeline.add(DummyLoader())
    result = pipeline.run()
    assert result is False

def test_books_raw_loader_load():
    mock_config = MagicMock()
    mock_config.paths.bronze_layer = "/tmp/bronze"
    
    loader = BooksRawLoader(mock_config)
    loader.minio = MagicMock()
    loader.minio.ensure_bucket_exists.return_value = None
    loader.minio.upload_directory.return_value = (1, 1)
    
    with patch('pathlib.Path.exists', return_value=True):
        result = loader.load()
        assert result is True

def test_books_transformer_transform():
    mock_config = MagicMock()
    transformer = BooksTransformer(mock_config)
    
    # Mock la SparkManager
    mock_spark_session = MagicMock()
    transformer.spark_manager = MagicMock()
    transformer.spark_manager.get_session.return_value = mock_spark_session
    
    # Mock la lecture et les transformations
    mock_df = MagicMock()
    mock_df.count.return_value = 100
    
    transformer._read_raw_data = MagicMock(return_value=mock_df)
    transformer._clean_books = MagicMock(return_value=mock_df)
    transformer._validate_books = MagicMock(return_value=mock_df)
    transformer._write_silver_layer = MagicMock(return_value=None)
    
    result = transformer.transform()
    assert result is True

def test_csv_extractor_extract():
    mock_config = MagicMock()
    mock_config.paths.csv_source = "/tmp/test_books.csv"
    mock_config.paths.bronze_layer = "/tmp/bronze"
    
    extractor = CSVExtractor(mock_config)
    
    mock_df = MagicMock()
    mock_df.__len__.return_value = 10
    mock_df.columns = ["title", "author"]
    
    with patch('pandas.read_csv', return_value=mock_df):
        with patch('pathlib.Path.mkdir'):
            with patch('pathlib.Path.exists', return_value=True):
                result = extractor.extract()
                assert result is True
