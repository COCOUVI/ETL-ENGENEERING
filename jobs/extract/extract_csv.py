"""
PHASE ETL : EXTRACT - Source CSV
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

CSV_SOURCE_PATH = Path("/opt/airflow/data/source/Books.csv")
BRONZE_OUTPUT_PATH = Path("/opt/airflow/data/bronze-layer/books")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# EXTRACT
# ============================================================

def extract_to_csv():
    """
    Lit le fichier CSV source et le sauvegarde dans le bronze-layer
    """
    try:
        logging.info(f"Reading CSV from {CSV_SOURCE_PATH}")
        df = pd.read_csv(CSV_SOURCE_PATH, sep=";", encoding="latin-1", on_bad_lines="skip")
        logging.info(f"CSV loaded : {len(df)} rows")

        BRONZE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

        output_file = BRONZE_OUTPUT_PATH / f"books_csv_{datetime.utcnow().date()}.csv"
        df.to_csv(output_file, index=False)
        logging.info(f"Saved to {output_file}")

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    extract_to_csv()