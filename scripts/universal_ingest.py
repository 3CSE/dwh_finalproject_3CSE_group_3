import os
from datetime import datetime
import logging
from psycopg2.extras import execute_values
from scripts.file_loader import load_file
from scripts.database_connection import get_connection

"""
Universal ingestion script to handle all ingestion pipelines.

- Preserves original data types from source files.
- Adds metadata columns: source_filename, ingestion_date.
- Supports CSV, Parquet, Pickle, JSON, HTML, Excel.
- Handles batch inserts efficiently.
- Meant to be called from specific ingestion scripts.
"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def batch_insert(cur, table_name: str, data_tuples: list, insert_cols: list, batch_size: int = 5000):
    """Insert data in batches to avoid memory issues."""
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
            batch
        )

def ingest(file_paths, table_name, required_cols, batch_size=5000, truncate=True):
    """
    Generic ingestion function.
    
    Parameters:
    - file_paths: str or list of files to ingest
    - table_name: target staging table
    - required_cols: list of columns that must exist in file
    - batch_size: number of rows per insert batch
    - truncate: whether to truncate table before insert
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    logging.info(f"Starting ingestion into table {table_name}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error(f"Cannot connect to database: {e}")
        return

    total_rows_inserted = 0

    try:
        if truncate:
            logging.info(f"Truncating table {table_name}")
            cur.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()

        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")

            # Load file and preserve original data types
            df = load_file(file_path)

            # Normalize column names (strip spaces, lowercase, replace spaces with _)
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            # Validate required columns
            for col in required_cols:
                if col.lower() not in df.columns:
                    raise KeyError(f"Required column '{col}' missing in file {file_path}")

            # Add metadata columns
            df["source_filename"] = os.path.basename(file_path)
            df["ingestion_date"] = datetime.now()

            insert_cols = [c.lower() for c in required_cols] + ["source_filename", "ingestion_date"]
            data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

            # Batch insert into database
            batch_insert(cur, table_name, data_tuples, insert_cols, batch_size)
            conn.commit()

            logging.info(f"{os.path.basename(file_path)}: Inserted {len(data_tuples)} rows")
            total_rows_inserted += len(data_tuples)

        logging.info(f"Ingestion complete — Total rows inserted: {total_rows_inserted}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    logging.warning("This script is a library. Use specific ingestion scripts to call 'ingest()'.")
