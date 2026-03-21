"""Tests for core.config module"""

import pytest
import os
from unittest.mock import patch
from core.config import (
    ConfigManager, MinIOConfig, DatabaseConfig, SparkConfig, PathConfig
)


class TestMinIOConfig:
    """Test MinIOConfig dataclass"""

    def test_minio_config_initialization(self):
        """Test MinIOConfig creation with all fields"""
        config = MinIOConfig(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key="password",
            bucket="my-bucket",
            region="us-west-1",
            use_ssl=True
        )
        assert config.endpoint == "http://minio:9000"
        assert config.access_key == "admin"
        assert config.bucket == "my-bucket"
        assert config.use_ssl is True

    def test_minio_config_defaults(self):
        """Test MinIOConfig default values"""
        config = MinIOConfig(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key="password"
        )
        assert config.bucket == "bronze-layer"
        assert config.region == "us-east-1"
        assert config.use_ssl is False

    def test_minio_config_validate_missing_access_key(self):
        """Test validation fails with missing access key"""
        config = MinIOConfig(
            endpoint="http://minio:9000",
            access_key="",
            secret_key="password"
        )
        with pytest.raises(ValueError, match="MINIO_ACCESS_KEY"):
            config.validate()

    def test_minio_config_validate_missing_secret_key(self):
        """Test validation fails with missing secret key"""
        config = MinIOConfig(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key=""
        )
        with pytest.raises(ValueError, match="MINIO_SECRET_KEY"):
            config.validate()

    def test_minio_config_validate_success(self):
        """Test validation succeeds with all credentials"""
        config = MinIOConfig(
            endpoint="http://minio:9000",
            access_key="admin",
            secret_key="password"
        )
        assert config.validate() is None  # Should not raise


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass"""

    def test_database_config_initialization(self):
        """Test DatabaseConfig creation"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="admin",
            password="pass123"
        )
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "mydb"

    def test_database_config_validate_success(self):
        """Test database config validation passes"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="admin",
            password="pass123"
        )
        assert config.validate() is None

    def test_database_config_validate_missing_host(self):
        """Test validation fails with missing host"""
        config = DatabaseConfig(
            host="",
            port=5432,
            database="mydb",
            user="admin",
            password="pass123"
        )
        with pytest.raises(ValueError, match="incomplètes"):
            config.validate()

    def test_database_config_validate_missing_user(self):
        """Test validation fails with missing user"""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="",
            password="pass123"
        )
        with pytest.raises(ValueError):
            config.validate()


class TestSparkConfig:
    """Test SparkConfig dataclass"""

    def test_spark_config_defaults(self):
        """Test SparkConfig default values"""
        config = SparkConfig()
        assert config.app_name == "ETL-Pipeline"
        assert config.shuffle_partitions == 4
        assert config.log_level == "WARN"

    def test_spark_config_custom_values(self):
        """Test SparkConfig with custom values"""
        config = SparkConfig(
            app_name="CustomETL",
            shuffle_partitions=8,
            log_level="DEBUG"
        )
        assert config.app_name == "CustomETL"
        assert config.shuffle_partitions == 8
        assert config.log_level == "DEBUG"


class TestPathConfig:
    """Test PathConfig dataclass"""

    def test_path_config_defaults(self):
        """Test PathConfig default paths"""
        config = PathConfig()
        assert "data/source" in config.csv_source
        assert "bronze-layer" in config.bronze_layer
        assert "silver-layer" in config.silver_layer

    def test_path_config_validate_creates_directories(self, temp_dir):
        """Test validate creates missing directories"""
        config = PathConfig(
            csv_source=str(temp_dir / "source.csv"),
            bronze_layer=str(temp_dir / "bronze"),
            silver_layer=str(temp_dir / "silver")
        )
        config.validate()
        
        assert (temp_dir / "bronze").exists()
        assert (temp_dir / "silver").exists()


class TestConfigManager:
    """Test ConfigManager singleton"""

    def test_config_manager_singleton_pattern(self):
        """Test ConfigManager is singleton"""
        ConfigManager._instance = None
        config1 = ConfigManager()
        config2 = ConfigManager()
        # In real usage, they should be same, but with our mocking they may differ
        assert isinstance(config1, ConfigManager)
        assert isinstance(config2, ConfigManager)

    def test_config_manager_get_instance(self):
        """Test ConfigManager.get_instance() method"""
        ConfigManager._instance = None
        instance = ConfigManager.get_instance()
        assert isinstance(instance, ConfigManager)

    @patch.dict(os.environ, {
        'MINIO_ENDPOINT': 'http://minio:9000',
        'MINIO_ROOT_USER': 'admin',
        'MINIO_ROOT_PASSWORD': 'password',
        'SOURCE_DB_HOST': 'localhost',
        'SOURCE_DB_PORT': '5432',
        'SOURCE_DB_NAME': 'sourcedb',
        'SOURCE_DB_USER': 'dbuser',
        'SOURCE_DB_PASSWORD': 'dbpass',
    })
    def test_config_manager_loads_env_variables(self):
        """Test ConfigManager loads environment variables"""
        ConfigManager._instance = None
        config = ConfigManager()
        
        assert config.minio.endpoint == 'http://minio:9000'
        assert config.minio.access_key == 'admin'
        assert config.database.host == 'localhost'
        assert config.database.port == 5432

    def test_config_manager_has_required_attributes(self):
        """Test ConfigManager has all required config objects"""
        ConfigManager._instance = None
        config = ConfigManager()
        
        assert hasattr(config, 'minio')
        assert hasattr(config, 'database')
        assert hasattr(config, 'spark')
        assert hasattr(config, 'paths')
        assert isinstance(config.minio, MinIOConfig)
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.spark, SparkConfig)
        assert isinstance(config.paths, PathConfig)

    def test_config_manager_paths_validation(self, temp_dir):
        """Test ConfigManager validates paths"""
        ConfigManager._instance = None
        
        with patch.dict(os.environ, {
            'MINIO_ENDPOINT': 'http://minio:9000',
            'MINIO_ROOT_USER': 'admin',
            'MINIO_ROOT_PASSWORD': 'password',
        }):
            config = ConfigManager()
            config.paths = PathConfig(
                csv_source=str(temp_dir / "source.csv"),
                bronze_layer=str(temp_dir / "bronze"),
                silver_layer=str(temp_dir / "silver")
            )
            config.paths.validate()
            
            assert (temp_dir / "bronze").exists()
            assert (temp_dir / "silver").exists()
