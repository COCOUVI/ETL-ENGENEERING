"""
PHASE ETL : EXTRACT - Source CSV

Refactored with OOP pattern using core modules
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

logger = get_logger(__name__)


class CSVExtractor(Extractor):
    """
    Extracteur CSV - Lit le fichier CSV source et le sauvegarde en bronze-layer
    
    Utilise les modules core:
    - ConfigManager pour les chemins centralisés
    - Logger pour logging standardisé
    - Extractor base class pour contrat ETL
    """
    
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Args:
            config: ConfigManager instance (utilise singleton si None)
        """
        super().__init__(name="CSVExtractor")
        self.config = config or ConfigManager.get_instance()
    
    def extract(self) -> bool:
        """
        Extrait le CSV source et le sauvegarde en bronze-layer
        
        Returns:
            bool: True si extraction réussie, False sinon
        """
        try:
            csv_path = self.config.paths.csv_source
            self.log_info(f"Reading CSV from {csv_path}")
            
            # Lecture du CSV source
            df = pd.read_csv(
                csv_path,
                sep=",",
                encoding="latin-1",
                on_bad_lines="skip",
                quotechar='"'
            )
            self.log_info(f"✓ CSV loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Création du dossier output
            output_dir = Path(self.config.paths.bronze_layer) / "books"
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Sauvegarde en bronze-layer
            output_file = output_dir / f"books_csv_{datetime.utcnow().date()}.csv"
            df.to_csv(output_file, index=False, sep=";")
            self.log_info(f"✓ Saved to {output_file}")
            
            return True
            
        except Exception as e:
            self.log_error(f"Extraction failed: {e}")
            return False


if __name__ == "__main__":
    # Setup logging (une fois au démarrage)
    setup_logging()
    
    # Créer et exécuter l'extracteur
    extractor = CSVExtractor()
    success = extractor.execute()
    
    # Exit avec code approprié
    import sys
    sys.exit(0 if success else 1)