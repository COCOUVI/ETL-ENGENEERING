"""SIMPLIFIED Tests for core module - Focus on coverage and simplicity"""

import pytest
import tempfile
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch
from core.config import ConfigManager, MinIOConfig, DatabaseConfig, SparkConfig, PathConfig
from core.base import ETLComponent, Extractor, Transformer, Loader, Pipeline
from core.logger import setup_logging, get_logger


# ==============================================================================
# CONFIG TESTS
# ==============================================================================

class TestConfigBasics:
    """Basic configuration tests"""

    def test_minio_config_creation(self):
        """Test creating MinIO config"""
        config = MinIOConfig(
            endpoint="http://localhost:9000",
            access_key="admin",
            secret_key="password"
        )
        assert config.endpoint == "http://localhost:9000"
        assert config.access_key == "admin"

    def test_minio_config_validation_fails(self):
        """Test validation fails without credentials"""
        config = MinIOConfig(endpoint="url", access_key="", secret_key="")
        with pytest.raises(ValueError):
            config.validate()

    def test_database_config_creation(self):
        """Test creating database config"""
        config = DatabaseConfig(
            host="localhost", port=5432, database="db",
            user="admin", password="pass"
        )
        assert config.host == "localhost"
        assert config.port == 5432

    def test_spark_config_defaults(self):
        """Test Spark config defaults"""
        config = SparkConfig()
        assert config.app_name == "ETL-Pipeline"
        assert config.shuffle_partitions == 4

    def test_path_config_creates_dirs(self, tmp_path):
        """Test path config creates directories"""
        config = PathConfig(
            csv_source=str(tmp_path / "source.csv"),
            bronze_layer=str(tmp_path / "bronze"),
            silver_layer=str(tmp_path / "silver")
        )
        config.validate()
        assert (tmp_path / "bronze").exists()
        assert (tmp_path / "silver").exists()


# ==============================================================================
# LOGGER TESTS
# ==============================================================================

class TestLoggerBasics:
    """Basic logger tests"""

    def test_setup_logging_returns_logger(self):
        """Test setup_logging returns logger"""
        logger = setup_logging()
        assert logger is not None
        assert hasattr(logger, 'info')

    def test_get_logger_returns_logger(self):
        """Test get_logger returns logger"""
        logger = get_logger("test")
        assert logger is not None
        assert logger.name == "test"

    def test_logger_can_log(self):
        """Test logger can log messages"""
        logger = get_logger("test2")
        try:
            logger.info("Test message")
            logger.error("Error message")
            logger.warning("Warning message")
        except Exception as e:
            pytest.fail(f"Logger failed: {e}")

    def test_setup_logging_with_file(self, tmp_path):
        """Test setup_logging with file"""
        log_file = tmp_path / "test.log"
        logger = setup_logging(log_file=str(log_file))
        logger.info("Test")
        assert log_file.exists()


# ==============================================================================
# BASE CLASSES TESTS
# ==============================================================================

class SimpleExtractor(Extractor):
    def extract(self):
        return True

class SimpleTransformer(Transformer):
    def transform(self):
        return True

class SimpleLoader(Loader):
    def load(self):
        return True


class TestETLBasics:
    """Basic ETL component tests"""

    def test_extractor_execute(self):
        """Test extractor execute"""
        ext = SimpleExtractor("TestExt")
        assert ext.execute() == True

    def test_transformer_execute(self):
        """Test transformer execute"""
        tran = SimpleTransformer("TestTran")
        assert tran.execute() == True

    def test_loader_execute(self):
        """Test loader execute"""
        loader = SimpleLoader("TestLoad")
        assert loader.execute() == True

    def test_pipeline_empty(self):
        """Test empty pipeline"""
        pipeline = Pipeline("Empty")
        assert pipeline.execute() == True

    def test_pipeline_with_stages(self):
        """Test pipeline with stages"""
        pipeline = Pipeline("Full")
        pipeline.add(SimpleExtractor("E"))
        pipeline.add(SimpleTransformer("T"))
        pipeline.add(SimpleLoader("L"))
        assert pipeline.execute() == True

    def test_pipeline_stops_on_failure(self):
        """Test pipeline stops on failure"""
        class FailingExtractor(Extractor):
            def extract(self):
                return False

        pipeline = Pipeline("Fail")
        pipeline.add(FailingExtractor("Fail"))
        pipeline.add(SimpleTransformer("T"))
        assert pipeline.execute() == False


# ==============================================================================
# CSV EXTRACTOR TESTS
# ==============================================================================

class TestCSVExtractorSimple:
    """Simple CSV extractor tests"""

    def test_extract_csv_file(self, tmp_path):
        """Test extracting CSV file"""
        try:
            import pandas as pd
            from jobs.extract.extract_csv import CSVExtractor
            
            # Create test CSV
            csv_file = tmp_path / "test.csv"
            df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
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
            assert (tmp_path / "bronze" / "books").exists()
        except ImportError:
            pytest.skip("pandas not available")


# ==============================================================================
# INTEGRATION METRICS
# ==============================================================================

class TestIntegration:
    """Integration tests"""

    def test_config_and_logger_work_together(self):
        """Test config and logger work together"""
        logger = get_logger("integration")
        config = ConfigManager()
        assert config is not None
        logger.info("Config loaded")

    def test_etl_pipeline_with_all_components(self):
        """Test ETL pipeline with all components"""
        pipeline = Pipeline("Complete")
        pipeline.add(SimpleExtractor("E"))
        pipeline.add(SimpleTransformer("T"))
        pipeline.add(SimpleLoader("L"))
        
        result = pipeline.execute()
        assert result == True

    def test_multiple_pipelines_independent(self):
        """Test multiple pipelines are independent"""
        p1 = Pipeline("P1")
        p1.add(SimpleExtractor("E1"))
        
        p2 = Pipeline("P2")
        p2.add(SimpleExtractor("E2"))
        
        assert p1.execute() == True
        assert p2.execute() == True
        assert p1.name != p2.name
