"""
MinIO S3 Client - Encapsule toute la logique MinIO (DRY)
Remplace les 5+ implémentations dupliquées dans le projet
"""

import logging
from pathlib import Path
from typing import Optional, Tuple
import boto3
from botocore.exceptions import ClientError
from core.config import ConfigManager, MinIOConfig


logger = logging.getLogger(__name__)


class MinIOClient:
    """
    Client MinIO réutilisable - OOP abstraction
    
    Avantages:
    - Encapsulation de ALL MinIO logic
    - Réutilisable partout dans le projet
    - Testable facilement
    - Centralisé = facile à maintenir
    
    Usage:
        config = ConfigManager.get_instance()
        client = MinIOClient(config.minio)
        client.ensure_bucket_exists()
        client.upload_file("local_file.json", "s3_key")
    """
    
    def __init__(self, config: Optional[MinIOConfig] = None):
        """
        Args:
            config: MinIOConfig instance. Si None, charge depuis ConfigManager
        """
        if config is None:
            config = ConfigManager.get_instance().minio
        
        self.config = config
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization du client S3"""
        if self._client is None:
            self._client = self._create_client()
            logger.info(f"MinIO client créé: {self.config.endpoint}")
        return self._client
    
    def _create_client(self):
        """Crée un client S3 compatible MinIO"""
        return boto3.client(
            "s3",
            endpoint_url=self.config.endpoint,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=self.config.region,
            use_ssl=self.config.use_ssl,
        )
    
    def ensure_bucket_exists(self, bucket: Optional[str] = None) -> None:
        """
        Vérifie que le bucket existe, le crée si nécessaire
        
        Args:
            bucket: Nom du bucket. Si None, utilise self.config.bucket
        """
        bucket_name = bucket or self.config.bucket
        
        try:
            self.client.head_bucket(Bucket=bucket_name)
            logger.info(f"✓ Bucket '{bucket_name}' existe")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.info(f"Bucket '{bucket_name}' non trouvé → création...")
                self.client.create_bucket(Bucket=bucket_name)
                logger.info(f"✓ Bucket '{bucket_name}' créé")
            else:
                raise
    
    def bucket_exists(self, bucket: Optional[str] = None) -> bool:
        """Vérifie l'existence d'un bucket"""
        bucket_name = bucket or self.config.bucket
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False
    
    def folder_exists(self, bucket: str, prefix: str) -> bool:
        """Vérifie si un dossier (prefix) contient des objets"""
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )
            return "Contents" in response and len(response["Contents"]) > 0
        except ClientError:
            return False
    
    def upload_file(
        self,
        local_path: Path,
        s3_key: str,
        bucket: Optional[str] = None,
    ) -> bool:
        """
        Upload un fichier vers MinIO
        
        Args:
            local_path: Chemin du fichier local
            s3_key: Clé S3 (path dans le bucket)
            bucket: Bucket cible (optionnel)
        
        Returns:
            bool: True si succès, False sinon
        """
        if isinstance(local_path, str):
            local_path = Path(local_path)
        
        bucket_name = bucket or self.config.bucket
        
        if not local_path.exists():
            logger.error(f"Fichier non trouvé: {local_path}")
            return False
        
        try:
            file_size_kb = local_path.stat().st_size / 1024
            self.client.upload_file(
                Filename=str(local_path),
                Bucket=bucket_name,
                Key=s3_key
            )
            logger.info(
                f"✓ Uploadé: s3://{bucket_name}/{s3_key} ({file_size_kb:.2f} KB)"
            )
            return True
        except Exception as e:
            logger.error(f"✗ Erreur upload {s3_key}: {e}")
            return False
    
    def upload_directory(
        self,
        local_dir: Path,
        s3_prefix: str,
        extension: str = "*",
        bucket: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        Upload tous les fichiers d'un dossier vers MinIO
        
        Args:
            local_dir: Dossier source
            s3_prefix: Préfixe S3 (ex: 'books/', 'sql/')
            extension: Extension de fichiers à uploader (ex: '*.json')
            bucket: Bucket cible
        
        Returns:
            Tuple[int, int]: (uploaded_count, total_count)
        """
        if isinstance(local_dir, str):
            local_dir = Path(local_dir)
        
        bucket_name = bucket or self.config.bucket
        
        if not local_dir.exists():
            logger.warning(f"Dossier non trouvé: {local_dir}")
            return 0, 0
        
        uploaded = 0
        total = 0
        
        pattern = f"**/{extension}" if extension != "*" else "**/*"
        
        for file_path in local_dir.glob(pattern):
            if not file_path.is_file():
                continue
            
            total += 1
            relative_path = file_path.relative_to(local_dir)
            s3_key = f"{s3_prefix}{relative_path}".replace("\\", "/")
            
            if self.upload_file(file_path, s3_key, bucket_name):
                uploaded += 1
        
        return uploaded, total
    
    def download_file(
        self,
        s3_key: str,
        local_path: Path,
        bucket: Optional[str] = None,
    ) -> bool:
        """
        Télécharge un fichier depuis MinIO
        
        Args:
            s3_key: Clé S3
            local_path: Chemin de destination local
            bucket: Bucket source
        
        Returns:
            bool: True si succès
        """
        if isinstance(local_path, str):
            local_path = Path(local_path)
        
        bucket_name = bucket or self.config.bucket
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.client.download_file(
                Bucket=bucket_name,
                Key=s3_key,
                Filename=str(local_path)
            )
            logger.info(f"✓ Téléchargé: {s3_key} → {local_path}")
            return True
        except Exception as e:
            logger.error(f"✗ Erreur téléchargement {s3_key}: {e}")
            return False
    
    def list_objects(
        self,
        prefix: str = "",
        bucket: Optional[str] = None,
    ) -> list:
        """Liste tous les objets d'un préfixe"""
        bucket_name = bucket or self.config.bucket
        objects = []
        
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' in page:
                    objects.extend([obj['Key'] for obj in page['Contents']])
            
            return objects
        except Exception as e:
            logger.error(f"Erreur listage {prefix}: {e}")
            return []
    
    def delete_object(
        self,
        s3_key: str,
        bucket: Optional[str] = None,
    ) -> bool:
        """Supprime un objet de MinIO"""
        bucket_name = bucket or self.config.bucket
        
        try:
            self.client.delete_object(Bucket=bucket_name, Key=s3_key)
            logger.info(f"✓ Supprimé: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"✗ Erreur suppression {s3_key}: {e}")
            return False
    
    def __repr__(self) -> str:
        return f"MinIOClient(endpoint={self.config.endpoint}, bucket={self.config.bucket})"
