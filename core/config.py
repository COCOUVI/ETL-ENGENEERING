"""
Configuration Management - Single source of truth pour les configs
Applique le principe DRY: charger les config UNE SEULE FOIS
"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv


@dataclass
class MinIOConfig:
    """Configuration MinIO"""
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "bronze-layer"
    region: str = "us-east-1"
    use_ssl: bool = False

    def validate(self) -> None:
        """Valide que toutes les configurations requises sont présentes"""
        if not self.access_key:
            raise ValueError("MINIO_ACCESS_KEY est requis")
        if not self.secret_key:
            raise ValueError("MINIO_SECRET_KEY est requis")


@dataclass
class DatabaseConfig:
    """Configuration PostgreSQL"""
    host: str
    port: int
    database: str
    user: str
    password: str

    def validate(self) -> None:
        """Valide les credentials DB"""
        if not all([self.host, self.user, self.password, self.database]):
            raise ValueError("Configurations DB incomplètes")


@dataclass
class SparkConfig:
    """Configuration Spark"""
    app_name: str = "ETL-Pipeline"
    jar_path: str = "/opt/airflow/hadoop-aws-3.2.4.jar,/opt/airflow/aws-java-sdk-bundle-1.11.1026.jar"
    shuffle_partitions: int = 4
    log_level: str = "WARN"


@dataclass
class PathConfig:
    """Configuration des chemins locaux"""
    csv_source: str = "/opt/airflow/data/source/Books.csv"
    bronze_layer: str = "/opt/airflow/data/bronze-layer"
    silver_layer: str = "/opt/airflow/data/silver-layer"
    
    def validate(self) -> None:
        """Crée les dossiers s'ils n'existent pas"""
        import os
        os.makedirs(self.bronze_layer, exist_ok=True)
        os.makedirs(self.silver_layer, exist_ok=True)


class ConfigManager:
    """
    Gestionnaire de configuration centralisé (Singleton pattern)
    Charge UNE SEULE FOIS à partir de .env
    
    Usage:
        config = ConfigManager()
        minio_config = config.minio
        db_config = config.database
    """
    
    _instance: Optional['ConfigManager'] = None
    
    def __init__(self):
        """Initialise et charge toutes les configurations"""
        # Charger les variables d'environnement
        load_dotenv(override=False)
        
        # MinIO configuration
        self.minio = MinIOConfig(
            endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", ""),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", ""),
            bucket=os.getenv("MINIO_BUCKET", "bronze-layer"),
        )
        
        # Database configuration
        self.database = DatabaseConfig(
            host=os.getenv("SOURCE_DB_HOST", "postgres-source"),
            port=int(os.getenv("SOURCE_DB_PORT", "5432")),
            database=os.getenv("SOURCE_DB_NAME", "sourcedb"),
            user=os.getenv("SOURCE_DB_USER", "sourceuser"),
            password=os.getenv("SOURCE_DB_PASSWORD", "sourcepass"),
        )
        
        # Spark configuration
        self.spark = SparkConfig(
            app_name=os.getenv("SPARK_APP_NAME", "ETL-Pipeline"),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
        )
        
        # Paths configuration
        self.paths = PathConfig(
            csv_source=os.getenv("CSV_SOURCE_PATH", "/opt/airflow/data/source/Books.csv"),
            bronze_layer=os.getenv("BRONZE_LAYER_PATH", "/opt/airflow/data/bronze-layer"),
            silver_layer=os.getenv("SILVER_LAYER_PATH", "/opt/airflow/data/silver-layer"),
        )
        
        # Validate
        self.minio.validate()
        self.database.validate()
        self.paths.validate()
    
    @classmethod
    def get_instance(cls) -> 'ConfigManager':
        """Singleton pattern - retourne l'unique instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __repr__(self) -> str:
        return (
            f"ConfigManager(\n"
            f"  minio={self.minio.endpoint},\n"
            f"  database={self.database.host}:{self.database.port},\n"
            f"  spark_app={self.spark.app_name}\n"
            f")"
        )


# Convenience: permettre d'importer directement
config = ConfigManager.get_instance()
