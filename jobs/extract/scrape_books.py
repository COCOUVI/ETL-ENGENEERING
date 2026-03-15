"""
PHASE ETL : EXTRACT - Web Scraping (books.toscrape.com)

Refactored with OOP pattern:
- Utilise Extractor base class (contrat ETL)
- Utilise ConfigManager (paths centralisés)
- Utilise Logger centralisé
- Type hints complètes
"""

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from core.base import Extractor
from core.config import ConfigManager
from core.logger import get_logger, setup_logging

logger = get_logger(__name__)


class BookScraper(Extractor):
    """
    Scraper pour books.toscrape.com
    
    Scrape les données de livres et les sauvegarde en JSON dans bronze-layer
    """
    
    # Configuration du scraping
    BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"
    HEADERS = {"User-Agent": "Mozilla/5.0"}
    REQUEST_TIMEOUT = 10
    MAX_PAGES = 50
    
    def __init__(self, config: Optional[ConfigManager] = None):
        """
        Args:
            config: ConfigManager instance (utilise singleton si None)
        """
        super().__init__(name="BookScraper")
        self.config = config or ConfigManager.get_instance()
    
    def extract(self) -> bool:
        """
        Scrape le site et sauvegarde les données en bronze-layer
        
        Returns:
            bool: True si scraping réussi, False sinon
        """
        try:
            # Scrape toutes les pages
            all_books = self._scrape_all_pages()
            
            if not all_books:
                self.log_error("No books scraped")
                return False
            
            # Sauvegarde les données
            self._save_raw_data(all_books)
            
            self.log_info(f"✓ Scraping completed: {len(all_books)} books")
            return True
            
        except Exception as e:
            self.log_error(f"Scraping failed: {e}")
            return False
    
    def _scrape_all_pages(self) -> List[Dict]:
        """
        Scrape l'ensemble des pages du site
        
        Returns:
            Liste de dictionnaires avec les données des livres
        """
        all_books = []
        
        for page in range(1, self.MAX_PAGES + 1):
            try:
                books = self._scrape_page(page)
                
                # Si aucune donnée, on arrête
                if not books:
                    self.log_info(f"No more pages after page {page}")
                    break
                
                all_books.extend(books)
                
            except Exception as e:
                self.log_warning(f"Page {page} failed: {e}")
                # Continue avec la page suivante
        
        return all_books
    
    def _scrape_page(self, page: int) -> List[Dict]:
        """
        Scrape une seule page
        
        Args:
            page: Numéro de page à scraper
        
        Returns:
            Liste de livres extraits de la page
        """
        url = self.BASE_URL.format(page)
        self.log_info(f"Scraping page {page}: {url}")
        
        # Requête HTTP
        response = requests.get(
            url,
            headers=self.HEADERS,
            timeout=self.REQUEST_TIMEOUT
        )
        response.raise_for_status()
        
        # Parsing HTML
        soup = BeautifulSoup(response.text, "html.parser")
        books = []
        
        # Extraction des livres
        for article in soup.select("article.product_pod"):
            book_data = self._extract_book_data(article)
            if book_data:
                books.append(book_data)
        
        return books
    
    def _extract_book_data(self, article) -> Optional[Dict]:
        """
        Extrait les données d'un livre depuis l'article HTML
        
        Args:
            article: BeautifulSoup article element
        
        Returns:
            Dict avec les données du livre, ou None si erreur
        """
        try:
            return {
                "title": article.h3.a["title"],
                "price": article.select_one(".price_color").text,
                "availability": article.select_one(".availability").text.strip(),
                "rating": self._parse_rating(article.p["class"][1]),
                "product_page_url": article.h3.a["href"],
                "scraped_at": datetime.utcnow().isoformat()
            }
        except Exception as e:
            self.log_warning(f"Error extracting book data: {e}")
            return None
    
    @staticmethod
    def _parse_rating(rating_class: str) -> str:
        """
        Convertit la classe CSS du rating en valeur lisible
        
        Example:
            'star-rating Three' → 'Three'
        """
        return rating_class.replace("star-rating", "").strip()
    
    def _save_raw_data(self, data: List[Dict]) -> None:
        """
        Sauvegarde les données en JSON dans bronze-layer
        
        Args:
            data: Liste des livres à sauvegarder
        """
        # Créer le dossier
        output_dir = Path(self.config.paths.bronze_layer) / "books"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Sauvegarder les données
        output_file = output_dir / f"books_{datetime.utcnow().date()}.json"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.log_info(f"✓ Raw data saved to {output_file}")


if __name__ == "__main__":
    # Setup logging (une fois au démarrage)
    setup_logging()
    
    # Créer et exécuter le scraper
    scraper = BookScraper()
    success = scraper.execute()
    
    # Exit avec code approprié
    import sys
    sys.exit(0 if success else 1)
