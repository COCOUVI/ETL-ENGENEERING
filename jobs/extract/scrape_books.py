import requests
from bs4 import BeautifulSoup
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict


# URL de base pour la pagination
BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

# Header HTTP pour éviter certains blocages
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Dossier de sortie pour les données RAW
RAW_DATA_PATH= Path("/opt/airflow/data/bronze-layer/books")
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

# Configuration du logging (standard industriel)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================
# FONCTIONS MÉTIER
# =========================

def parse_rating(rating_class: str) -> str:
    """
    Convertit la classe CSS du rating en valeur lisible.

    Exemple :
    'star-rating Three' -> 'Three'
    """
    return rating_class.replace("star-rating", "").strip()


def scrape_page(page: int) -> List[Dict]:
    """
    Scrape une page du site et extrait les informations des livres.

    :param page: Numéro de page à scraper
    :return: Liste de dictionnaires contenant les données des livres
    """
    url = BASE_URL.format(page)
    logging.info(f"Scraping page {page}")

    # Requête HTTP avec timeout pour éviter les blocages
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()

    # Parsing HTML avec BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")
    books = []

    # Parcours des livres présents sur la page
    for article in soup.select("article.product_pod"):
        books.append({
            "title": article.h3.a["title"],
            "price": article.select_one(".price_color").text,
            "availability": article.select_one(".availability").text.strip(),
            "rating": parse_rating(article.p["class"][1]),
            "product_page_url": article.h3.a["href"],
            "scraped_at": datetime.utcnow().isoformat()
        })

    return books


def scrape_all_pages(max_pages: int = 50) -> List[Dict]:
    """
    Scrape l'ensemble des pages du site jusqu'à la dernière page disponible.

    :param max_pages: Nombre maximal de pages à scraper (sécurité)
    :return: Liste complète des livres scrapés
    """
    all_books = []

    for page in range(1, max_pages + 1):
        try:
            books = scrape_page(page)

            # Si aucune donnée n'est retournée, on arrête le scraping
            if not books:
                break

            all_books.extend(books)

        except Exception as e:
            # Gestion des erreurs sans stopper le pipeline
            logging.error(f"Page {page} failed: {e}")

    logging.info(f"Total books scraped: {len(all_books)}")
    return all_books


def save_raw_data(data: List[Dict]) -> None:
    """
    Sauvegarde les données brutes au format JSON dans la zone RAW.

    :param data: Données à sauvegarder
    """
    output_file = RAW_DATA_PATH / f"books_{datetime.utcnow().date()}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logging.info(f"Raw data saved to {output_file}")

# =========================
# POINT D'ENTRÉE
# =========================

if __name__ == "__main__":
    # Lancement du scraping complet
    books = scrape_all_pages()

    # Sauvegarde des données brutes
    save_raw_data(books)
