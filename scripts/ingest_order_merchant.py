import os
import pandas as pd
from datetime import datetime
from database_connection import get_connection
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging

# Load environment variables from .env
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing the order_merchant data files
DATA_DIR = os.path.join("/app", "dataset", "enterprise_department")

# Scan folder for CSV and Parquet files
FILES = [
    os.path.join(DATA_DIR, f)
    for f in os.listdir(DATA_DIR)
    if f.lower().endswith(".csv") or f.lower().endswith(".parquet")
]

def ingest_order_merchant(file_path, table_name="staging.stg_order_merchant", batch_size=5000):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    try:
        # Read the file based on its extension
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext == ".parquet":
            df = pd.read_parquet(file_path)
        else:
            logging.warning(f"Unsupported file type: {file_path}")
            return

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Ensure required columns exist
        required_cols = ["order_id", "merchant_id", "staff_id"]
        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' not found in {file_path}")

        # Convert all to strings
        for col in required_cols:
            df[col] = df[col].astype(str)

        # Add metadata
        df["source_filename"] = file_path
        df["ingestion_date"] = datetime.now()

        # Prepare for batch insert
        insert_cols = required_cols + ["source_filename", "ingestion_date"]
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

        # Truncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        # Insert in batches
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            execute_values(
                cur,
                f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
                batch
            )
            conn.commit()
            logging.info(f"Inserted rows {i+1} to {i+len(batch)}")

        logging.info(f"Ingestion complete: {len(data_tuples)} rows inserted from {file_path}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    if not FILES:
        logging.warning(f"No CSV or Parquet files found in folder: {DATA_DIR}")
    else:
        for file in FILES:
            ingest_order_merchant(file)
