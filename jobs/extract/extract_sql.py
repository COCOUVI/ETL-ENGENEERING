"""
PHASE ETL : EXTRACT - Source PostgreSQL
Sauvegarde en format Parquet
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import psycopg2
import os

# ============================================================
# CONFIGURATION
# ============================================================

# Connexion PostgreSQL source
DB_HOST = os.getenv('SOURCE_DB_HOST', 'postgres-source')
DB_PORT = os.getenv('SOURCE_DB_PORT', '5432')
DB_NAME = os.getenv('SOURCE_DB_NAME', 'sourcedb')
DB_USER = os.getenv('SOURCE_DB_USER', 'sourceuser')
DB_PASSWORD = os.getenv('SOURCE_DB_PASSWORD', 'sourcepass')

BRONZE_OUTPUT_PATH = Path("/opt/airflow/data/bronze-layer/sql")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ============================================================
# EXTRACT
# ============================================================

def extract_to_parquet():
    """
    Lit les tables PostgreSQL et les sauvegarde en Parquet dans le bronze-layer
    """
    conn = None
    
    try:
        # Connexion à PostgreSQL
        logging.info(f"Connecting to PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("PostgreSQL connected successfully")
        
        # Tables à extraire
        tables = ['authors', 'categories', 'books']
        total_rows = 0
        
        for table_name in tables:
            logging.info(f"Reading table: {table_name}")
            
            # Lire la table avec pandas
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            rows = len(df)
            total_rows += rows
            logging.info(f"Table {table_name} loaded: {rows} rows, {len(df.columns)} columns")
            
            # Créer le dossier de sortie pour cette table
            table_output_path = BRONZE_OUTPUT_PATH / table_name
            table_output_path.mkdir(parents=True, exist_ok=True)
            
            # Sauvegarder en Parquet
            output_file = table_output_path / f"{table_name}_{datetime.utcnow().date()}.parquet"
            df.to_parquet(
                output_file, 
                index=False, 
                engine='pyarrow',
                coerce_timestamps='us',          # ← FIX: Microseconds
                allow_truncated_timestamps=True  # ← Permet la conversion
            )
            
            # Afficher la taille du fichier
            file_size = output_file.stat().st_size / 1024  # en KB
            logging.info(f"Saved to {output_file} ({file_size:.2f} KB)")
        
        logging.info(f"✓ Extraction completed: {total_rows} total rows from {len(tables)} tables")

    except psycopg2.Error as e:
        logging.error(f"PostgreSQL error: {e}")
        raise
    
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise
    
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    extract_to_parquet()