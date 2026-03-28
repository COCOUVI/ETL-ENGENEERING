"""
Base Classes ETL - Abstract base classes pour Extract/Transform/Load
Etablit l'architecture OOP et permet la réutilisabilité

Design Pattern:
- Strategy pattern pour différentes sources
- Template Method pour ETL workflow
- Abstract Base Classes pour interfaces
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class ETLComponent(ABC):
    """
    Base class abstraite pour tous les composants ETL
    
    Formel l'interface que CHAQUE composant doit implémenter:
    - Logging standardisé
    - Error handling cohérent
    - Documentation requise
    """
    
    def __init__(self, name: str):
        """
        Args:
            name: Nom du composant (utilisé pour logging)
        """
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def execute(self) -> bool:
        """
        Exécute le composant
        
        Returns:
            bool: True si succès, False sinon
        """
        pass
    
    def log_info(self, message: str) -> None:
        """Log un message info avec préfixe du composant"""
        self.logger.info(f"[{self.name}] {message}")
    
    def log_error(self, message: str) -> None:
        """Log un message erreur"""
        self.logger.error(f"[{self.name}] {message}")
    
    def log_warning(self, message: str) -> None:
        """Log un message warning"""
        self.logger.warning(f"[{self.name}] {message}")


class Extractor(ETLComponent):
    """
    Base class pour tous les extracteurs (E de ETL)
    
    Responsabilité:
    - Lire des données depuis une source
    - Valider les données
    - Les sauvegarder en bronze-layer
    """
    
    @abstractmethod
    def extract(self) -> bool:
        """Extrait les données"""
        pass
    
    def execute(self) -> bool:
        """Wrapper pour l'extraction"""
        try:
            self.log_info("Extraction en cours...")
            result = self.extract()
            if result:
                self.log_info("✓ Extraction terminée avec succès")
            else:
                self.log_error("✗ Extraction échouée")
            return result
        except Exception as e:
            self.log_error(f"Exception: {e}")
            raise


class Transformer(ETLComponent):
    """
    Base class pour tous les transformateurs (T de ETL)
    
    Responsabilité:
    - Lire données du bronze-layer
    - Nettoyer, enrichir, typer
    - Sauvegarder en silver-layer
    """
    
    @abstractmethod
    def transform(self) -> bool:
        """Transforme les données"""
        pass
    
    def execute(self) -> bool:
        """Wrapper pour la transformation"""
        try:
            self.log_info("Transformation en cours...")
            result = self.transform()
            if result:
                self.log_info("✓ Transformation terminée")
            else:
                self.log_error("✗ Transformation échouée")
            return result
        except Exception as e:
            self.log_error(f"Exception: {e}")
            raise


class Loader(ETLComponent):
    """
    Base class pour tous les loaders (L de ETL)
    
    Responsabilité:
    - Charger des données vers une destination
    - Bronze-layer vers MinIO, silver vers data warehouse, etc.
    """
    
    @abstractmethod
    def load(self) -> bool:
        """Charge les données"""
        pass
    
    def execute(self) -> bool:
        """Wrapper pour le chargement"""
        try:
            self.log_info("Chargement en cours...")
            result = self.load()
            if result:
                self.log_info("✓ Chargement terminé")
            else:
                self.log_error("✗ Chargement échoué")
            return result
        except Exception as e:
            self.log_error(f"Exception: {e}")
            raise


class DataSource(ABC):
    """
    Abstract base class pour les sources de données (Strategy Pattern)
    
    Usage:
        class CSVSource(DataSource):
            def read(self):
                return pd.read_csv(...)
        
        extractor = GenericExtractor(source=CSVSource())
        extractor.extract()
    """
    
    @abstractmethod
    def read(self) -> Any:
        """Lit les données de la source"""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Valide la source"""
        pass
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Retourne le nom de la source"""
        pass


class Pipeline:
    """
    Orchestrateur de pipeline ETL (Composite Pattern)
    Enchaîne plusieurs composants ETL
    
    Usage:
        pipeline = Pipeline("Books ETL")
        pipeline.add(scraper)
        pipeline.add(loader)
        pipeline.add(transformer)
        pipeline.execute()
    """
    
    def __init__(self, name: str):
        """
        Args:
            name: Nom du pipeline
        """
        self.name = name
        self.stages: list[ETLComponent] = []
        self.logger = logging.getLogger(f"{__name__}.Pipeline")
    
    def add(self, component: ETLComponent) -> 'Pipeline':
        """
        Ajoute un composant au pipeline
        
        Returns:
            self pour permettre le chaînage
        """
        self.stages.append(component)
        self.logger.debug(f"Composant ajouté: {component.name}")
        return self
    
    def execute(self) -> bool:
        """
        Exécute tous les composants du pipeline dans l'ordre
        
        Si un composant échoue, le pipeline s'arrête
        
        Returns:
            bool: True si tous les composants réussissent
        """
        self.logger.info(f"Démarrage pipeline: {self.name}")
        
        for i, stage in enumerate(self.stages, 1):
            self.logger.info(f"\n[{i}/{len(self.stages)}] Exécution: {stage.name}")
            try:
                if not stage.execute():
                    self.logger.error(f"Pipeline échoué au stage: {stage.name}")
                    return False
            except Exception as e:
                self.logger.error(f"Exception au stage {stage.name}: {e}")
                return False
        
        self.logger.info(f"✓ Pipeline '{self.name}' complété avec succès")
        return True
    
    def run(self) -> bool:
        """
        Alias for execute to match test expectations.
        """
        return self.execute()
    
    def __repr__(self) -> str:
        return f"Pipeline(name={self.name}, stages={len(self.stages)})"
