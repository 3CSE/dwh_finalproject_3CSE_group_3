import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging
from scripts.database_connection import get_connection

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing JSON file
DATA_DIR = os.path.join("/app", "dataset", "customer_management_department")
FILE_PATH = os.path.join(DATA_DIR, "user_data.json")

def ingest_user_data(file_path=FILE_PATH, table_name="staging.stg_user_data", batch_size=5000):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return


    try:
        # Truncate table
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        logging.info(f"Truncated table {table_name} before ingestion")
        
        # Load JSON
        df = pd.read_json(file_path)

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Example: minimal cleaning for staging
        for col in df.columns:
            df[col] = df[col].astype(str)

        # Add metadata
        df["source_filename"] = file_path
        df["ingestion_date"] = datetime.now()

        # Prepare for batch insert
        insert_cols = list(df.columns)
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

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
    ingest_user_data()
