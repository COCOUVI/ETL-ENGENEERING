"""
Spark Session Manager - Factory pattern pour Spark (DRY)
Centralise la création et configuration de SparkSession
Remplace les créations dupliquées dans transform_books.py et transform_csv_books.py
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession
from core.config import ConfigManager, SparkConfig, MinIOConfig


logger = logging.getLogger(__name__)


class SparkManager:
    """
    Gestionnaire de SparkSession (Singleton Factory)
    
    Avantages:
    - Crée une SEULE session Spark
    - Centralise la config Spark
    - Réutilisable partout
    
    Usage:
        spark_manager = SparkManager()
        spark = spark_manager.get_session()
        df = spark.read.csv("s3a://...")
    """
    
    _instance: Optional['SparkManager'] = None
    _session: Optional[SparkSession] = None
    
    def __init__(
        self,
        spark_config: Optional[SparkConfig] = None,
        minio_config: Optional[MinIOConfig] = None,
    ):
        """
        Args:
            spark_config: SparkConfig instance
            minio_config: MinIOConfig instance (pour S3A)
        """
        if spark_config is None:
            config = ConfigManager.get_instance()
            spark_config = config.spark
            minio_config = config.minio
        
        self.spark_config = spark_config
        self.minio_config = minio_config
    
    @classmethod
    def get_instance(cls) -> 'SparkManager':
        """Retourne l'unique instance (Singleton)"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def get_session(self) -> SparkSession:
        """
        Récupère ou crée la SparkSession
        
        Configuration incluante:
        - Support S3A (MinIO)
        - Credentials MinIO
        - Logging level
        - Nombre de partitions
        
        Returns:
            SparkSession configurée et prête à l'usage
        """
        if SparkManager._session is not None:
            return SparkManager._session
        
        logger.info("Initialisation SparkSession...")
        
        builder = SparkSession.builder \
            .appName(self.spark_config.app_name) \
            .config("spark.jars", self.spark_config.jar_path) \
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_config.endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_config.access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_config.secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.shuffle.partitions", str(self.spark_config.shuffle_partitions)) \
            .config("spark.default.parallelism", str(self.spark_config.shuffle_partitions))
        
        SparkManager._session = builder.getOrCreate()
        SparkManager._session.sparkContext.setLogLevel("ERROR")
        SparkManager._session.sparkContext.setLogLevel(self.spark_config.log_level)
        
        logger.info(f"✓ SparkSession créée: {self.spark_config.app_name}")
        self._log_session_info()
        
        return SparkManager._session
    
    def get_or_create_session(self) -> SparkSession:
        """
        Alias for get_session to match test expectations.
        """
        return self.get_session()
    
    def _log_session_info(self) -> None:
        """Log les infos de la session Spark"""
        session = SparkManager._session
        logger.info(f"  - Master: {session.sparkContext.master}")
        logger.info(f"  - App Name: {session.sparkContext.appName}")
        logger.info(f"  - Partitions: {self.spark_config.shuffle_partitions}")
        logger.info(f"  - S3A Endpoint: {self.minio_config.endpoint}")
    
    def stop_session(self) -> None:
        """Arrête la SparkSession (nettoyage)"""
        if SparkManager._session is not None:
            logger.info("Arrêt de SparkSession...")
            SparkManager._session.stop()
            SparkManager._session = None
            logger.info("✓ SparkSession arrêtée")
    
    def __repr__(self) -> str:
        return (
            f"SparkManager(app_name={self.spark_config.app_name}, "
            f"partitions={self.spark_config.shuffle_partitions})"
        )
