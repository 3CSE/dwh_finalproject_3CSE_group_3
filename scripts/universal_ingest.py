import os
from datetime import datetime
from psycopg2.extras import execute_values
import logging

from scripts.file_loader import load_file
from scripts.database_connection import get_connection

'''
Call this universal_ingest.py for every ingestion script.

'''

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# batch insert helper method
def batch_insert(cur, table_name: str, data_tuples: list, insert_cols: list, batch_size: int = 5000):
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
            batch
        )


def ingest(file_paths, table_name, required_cols, batch_size=5000):

    if isinstance(file_paths, str):
        file_paths = [file_paths]

    logging.info(f"Starting ingestion into table {table_name}")

    # Connect to database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot connect to database")
        return
    
    total_rows_inserted = 0

    try:
        # Truncate table 
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")

            # Load the file
            df = load_file(file_path)

            # Normalize column names
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            # Check required columns
            for col in required_cols:
                if col.lower() not in df.columns:
                    raise KeyError(f"Required column '{col}' missing in file {file_path}")

                df[col] = df[col].astype(str)  # Convert all to string for staging

            # Add metadata columns
            df["source_filename"] = os.path.basename(file_path)
            df["ingestion_date"] = datetime.now()

            insert_cols = [c.lower() for c in required_cols] + ["source_filename", "ingestion_date"]
            data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

            # Batch insert
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
