"""
Logger Centralisé - Configuration logging standardisée (DRY)
Remplace les 8+ logging.basicConfig() disséminés dans le projet
"""

import logging
from typing import Optional


# Constantes de logging
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
SIMPLE_FORMAT = "%(levelname)s - %(message)s"


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Configure le logging pour toute l'application
    
    Args:
        level: Niveau de logging (DEBUG, INFO, WARNING, ERROR)
        format_string: Format personnalisé (utilise DEFAULT_FORMAT si None)
        log_file: Chemin du fichier de log (optionnel)
    
    Returns:
        Logger configuré
    
    Usage:
        # Simple
        logger = setup_logging()
        logger.info("Message")
        
        # Avec fichier
        logger = setup_logging(log_file="app.log")
        
        # Debug
        logger = setup_logging(level=logging.DEBUG)
    """
    
    format_string = format_string or DEFAULT_FORMAT
    
    # Configuration du root logger
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=_get_handlers(log_file, format_string),
    )
    
    return logging.getLogger(__name__)


def _get_handlers(log_file: Optional[str], format_string: str) -> list:
    """Génère les handlers pour logging (console + fichier optionnel)"""
    
    handlers = []
    formatter = logging.Formatter(format_string)
    
    # Handler console (stdout)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # Handler fichier (optionnel)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    return handlers


def get_logger(
    name: str,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Récupère un logger nommé configuré
    
    Usage:
        logger = get_logger(__name__)
        logger.info("Message")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Si pas encore configuré, ajouter un handler par défaut
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(DEFAULT_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


# Logger application globale
app_logger = get_logger("etl-app")
