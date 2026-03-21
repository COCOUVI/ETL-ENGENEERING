"""Tests avancés pour core.implementations (Pipeline, Loader, Extractor, Transformer)"""

import pytest
from core.implementations import (
    BooksRawLoader, BooksTransformer, CSVExtractor, Pipeline
)
from unittest.mock import MagicMock, patch

class DummyExtractor:
    def __init__(self):
        self.name = "DummyExtractor"
    def extract(self):
        return "data"

class DummyTransformer:
    def __init__(self):
        self.name = "DummyTransformer"
    def transform(self, data):
        return data + "_transformed"

class DummyLoader:
    def __init__(self):
        self.name = "DummyLoader"
    def load(self, data):
        return data + "_loaded"

def test_pipeline_run_success():
    pipeline = Pipeline("Test Pipeline")
    pipeline.add(DummyExtractor())
    pipeline.add(DummyTransformer())
    pipeline.add(DummyLoader())
    result = pipeline.run()
    assert result == "data_transformed_loaded"

def test_pipeline_run_exception():
    class BadExtractor:
        def __init__(self):
            self.name = "BadExtractor"
        def extract(self):
            raise Exception("fail extract")
    pipeline = Pipeline("Test Pipeline")
    pipeline.add(BadExtractor())
    pipeline.add(DummyTransformer())
    pipeline.add(DummyLoader())
    with pytest.raises(Exception):
        pipeline.run()

def test_books_raw_loader_load():
    loader = BooksRawLoader(MagicMock())
    loader.minio = MagicMock()
    loader.minio.ensure_bucket_exists.return_value = None
    loader.minio.upload_directory.return_value = (1, 1)
    loader.config = MagicMock()
    loader.config.paths.bronze_layer = "/tmp"
    assert loader.load() is True

def test_books_transformer_transform():
    transformer = BooksTransformer(MagicMock())
    transformer.spark_manager = MagicMock()
    transformer.spark_manager.get_session.return_value = MagicMock()
    transformer._read_raw_data = MagicMock(return_value=MagicMock(count=MagicMock(return_value=1), columns=["title", "price", "availability"]))
    transformer._clean_books = MagicMock(return_value=MagicMock(count=MagicMock(return_value=1), columns=["title", "price", "availability"]))
    transformer._validate_books = MagicMock(return_value=MagicMock(count=MagicMock(return_value=1), columns=["title", "price", "availability"]))
    transformer._write_silver_layer = MagicMock()
    assert transformer.transform() is True

def test_csv_extractor_extract():
    extractor = CSVExtractor("/fake/path.csv")
    with patch('pandas.read_csv', return_value="df"):
        assert extractor.extract() == "df"
