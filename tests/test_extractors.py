"""Tests for jobs.extract module"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime
from jobs.extract.extract_csv import CSVExtractor
from core.config import ConfigManager, PathConfig


@pytest.fixture
def extractor_config(temp_dir):
    """Create config for extractor tests"""
    return ConfigManager()


class TestCSVExtractor:
    """Test CSVExtractor class"""

    def test_csv_extractor_initialization(self, extractor_config):
        """Test CSVExtractor initialization"""
        extractor = CSVExtractor(config=extractor_config)
        assert extractor.config == extractor_config
        assert extractor.name == "CSVExtractor"

    def test_csv_extractor_extract_success(self, temp_dir):
        """Test successful CSV extraction"""
        # Create test CSV file
        csv_file = temp_dir / "source.csv"
        df = pd.DataFrame({'title': ['Book1', 'Book2'], 'author': ['Author1', 'Author2']})
        df.to_csv(csv_file, index=False)
        
        # Setup config
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        # Extract
        extractor = CSVExtractor(config=config)
        result = extractor.extract()
        
        assert result is True
        # Check output file exists
        bronze_dir = temp_dir / "bronze" / "books"
        assert bronze_dir.exists()
        csv_files = list(bronze_dir.glob("*.csv"))
        assert len(csv_files) > 0

    def test_csv_extractor_extract_missing_file(self, temp_dir):
        """Test extraction fails when CSV doesn't exist"""
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(temp_dir / "nonexistent.csv"),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        
        extractor = CSVExtractor(config=config)
        result = extractor.extract()
        
        assert result is False

    def test_csv_extractor_execute_success(self, temp_dir):
        """Test execute method (wrapper) succeeds"""
        csv_file = temp_dir / "source.csv"
        df = pd.DataFrame({'title': ['Book1'], 'author': ['Author1']})
        df.to_csv(csv_file, index=False)
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        extractor = CSVExtractor(config=config)
        result = extractor.execute()
        
        assert result is True

    def test_csv_extractor_execute_failure(self, temp_dir):
        """Test execute method handles failure"""
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(temp_dir / "nonexistent.csv"),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        
        extractor = CSVExtractor(config=config)
        result = extractor.execute()
        
        assert result is False

    def test_csv_extractor_reads_latin_encoding(self, temp_dir):
        """Test extractor handles latin-1 encoding"""
        csv_file = temp_dir / "source.csv"
        df = pd.DataFrame({'title': ['Café', 'Naïve'], 'author': ['Écrivain', 'Tête']})
        df.to_csv(csv_file, index=False, encoding='latin-1')
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        extractor = CSVExtractor(config=config)
        result = extractor.extract()
        
        assert result is True

    def test_csv_extractor_saves_with_date(self, temp_dir):
        """Test extracted CSV includes date in filename"""
        csv_file = temp_dir / "source.csv"
        df = pd.DataFrame({'col': [1, 2]})
        df.to_csv(csv_file, index=False)
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        extractor = CSVExtractor(config=config)
        extractor.extract()
        
        bronze_dir = temp_dir / "bronze" / "books"
        csv_files = list(bronze_dir.glob("*.csv"))
        
        # Should have date in filename
        assert any(datetime.utcnow().date().isoformat() in str(f) for f in csv_files)


class TestCSVExtractorIntegration:
    """Integration tests for CSV extraction"""

    def test_full_extraction_pipeline(self, temp_dir):
        """Test full extraction pipeline"""
        # Setup
        csv_file = temp_dir / "source.csv"
        df = pd.DataFrame({
            'title': ['Book1', 'Book2', 'Book3'],
            'author': ['Author1', 'Author2', 'Author3'],
            'year': [2020, 2021, 2022]
        })
        df.to_csv(csv_file, index=False)
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        # Execute
        extractor = CSVExtractor(config=config)
        success = extractor.execute()
        
        assert success is True
        
        # Verify
        bronze_dir = temp_dir / "bronze" / "books"
        csv_files = list(bronze_dir.glob("*.csv"))
        assert len(csv_files) == 1
        
        # Read and verify data
        extracted = pd.read_csv(csv_files[0], sep=";")
        assert len(extracted) == 3
        assert list(extracted.columns) == ['title', 'author', 'year']

    def test_extraction_with_large_file(self, temp_dir):
        """Test extraction with larger CSV file"""
        csv_file = temp_dir / "source.csv"
        rows = 1000
        df = pd.DataFrame({
            'id': range(rows),
            'title': [f'Book{i}' for i in range(rows)],
            'author': [f'Author{i}' for i in range(rows)]
        })
        df.to_csv(csv_file, index=False)
        
        config = ConfigManager()
        config.paths = PathConfig(
            csv_source=str(csv_file),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.paths.validate()
        
        extractor = CSVExtractor(config=config)
        result = extractor.execute()
        
        assert result is True
