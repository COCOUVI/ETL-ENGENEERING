"""Tests for jobs module - Simplified integration tests"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class TestJobsIntegration:
    """Integration tests for jobs module"""

    @pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
    def test_csv_extractor_integration(self, tmp_path):
        """Test CSV extractor integration"""
        from jobs.extract.extract_csv import CSVExtractor
        from core.config import ConfigManager, PathConfig
        
        # Create test CSV
        csv_file = tmp_path / "books.csv"
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'title': ['Book1', 'Book2', 'Book3'],
            'author': ['Author1', 'Author2', 'Author3']
        })
        df.to_csv(csv_file, index=False)
        
        # Setup config
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(tmp_path / "bronze"),
            silver_layer=str(tmp_path / "silver")
        )
        config.paths.validate()
        
        # Extract
        extractor = CSVExtractor(config)
        result = extractor.execute()
        
        assert result == True
        # Verify output
        bronze_dir = tmp_path / "bronze" / "books"
        assert bronze_dir.exists()
        csv_files = list(bronze_dir.glob("*.csv"))
        assert len(csv_files) > 0

    def test_csv_extractor_no_file(self, tmp_path):
        """Test CSV extractor with missing file"""
        from jobs.extract.extract_csv import CSVExtractor
        from core.config import ConfigManager, PathConfig
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(tmp_path / "nonexistent.csv"),
            bronze_layer=str(tmp_path / "bronze"),
            silver_layer=str(tmp_path / "silver")
        )
        config.paths.validate()
        
        extractor = CSVExtractor(config)
        result = extractor.execute()
        
        assert result == False

    @patch('core.minio_client.MinIOClient')
    @pytest.mark.skip(reason="Already covered by test_loader_successful_upload in test_loaders.py")
    def test_csv_loader_integration(self):
        """Test CSV loader integration - basic instantiation"""
        pass

    def test_loader_no_files(self, tmp_path):
        """Test loader with no files"""
        from jobs.load.load_csv_to_minio import BooksRawCSVLoader
        from core.config import ConfigManager, PathConfig, MinIOConfig
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(tmp_path / "source.csv"),
            bronze_layer=str(tmp_path / "bronze"),
            silver_layer=str(tmp_path / "silver")
        )
        config.minio = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        
        loader = BooksRawCSVLoader(config)
        result = loader.execute()
        
        assert result == False

    @patch('core.minio_client.MinIOClient')
    @patch('core.spark_manager.SparkManager')
    def test_transformer_integration(self, mock_spark_class, mock_minio_class, tmp_path):
        """Test transformer integration"""
        from jobs.transform.transform_books import BooksRawTransformer
        from core.config import ConfigManager, PathConfig, MinIOConfig
        
        # Setup mocks
        mock_spark = MagicMock()
        mock_spark_class.return_value = mock_spark
        mock_session = MagicMock()
        mock_spark.get_session.return_value = mock_session
        
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_session.read.option.return_value.json.return_value = mock_df
        
        mock_minio = MagicMock()
        mock_minio_class.return_value = mock_minio
        
        # Setup config
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(tmp_path / "source.csv"),
            bronze_layer=str(tmp_path / "bronze"),
            silver_layer=str(tmp_path / "silver")
        )
        config.minio = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        
        # Transform
        transformer = BooksRawTransformer(config)
        assert transformer is not None


class TestJobsModuleBasics:
    """Basic module tests for jobs"""

    def test_extract_module_imports(self):
        """Test extract module can be imported"""
        from jobs.extract import extract_csv
        assert extract_csv is not None

    def test_load_module_imports(self):
        """Test load module can be imported"""
        from jobs.load import load_csv_to_minio
        assert load_csv_to_minio is not None

    def test_transform_module_imports(self):
        """Test transform module can be imported"""
        from jobs.transform import transform_books
        assert transform_books is not None

    def test_extract_csv_extractor_exists(self):
        """Test CSVExtractor class exists"""
        from jobs.extract.extract_csv import CSVExtractor
        assert CSVExtractor is not None

    def test_loader_exists(self):
        """Test BooksRawCSVLoader class exists"""
        from jobs.load.load_csv_to_minio import BooksRawCSVLoader
        assert BooksRawCSVLoader is not None

    def test_transformer_exists(self):
        """Test BooksRawTransformer class exists"""
        from jobs.transform.transform_books import BooksRawTransformer
        assert BooksRawTransformer is not None
