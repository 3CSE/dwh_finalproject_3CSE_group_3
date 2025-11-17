import os
import pandas as pd
from datetime import datetime
from psycopg2 import connect
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = os.path.join("/app", "dataset", "customer_management_department")
FILE_PATH = os.path.join(DATA_DIR, "user_credit_card.pickle")

def get_connection():
    """Connect to PostgreSQL using environment variables."""
    try:
        conn = connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            port=int(os.environ.get('DB_PORT', 5432))
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

def ingest_user_credit_card(table_name="staging.stg_user_credit_card", batch_size=5000):
    logging.info(f"Starting ingestion for {FILE_PATH} into {table_name}")

    # Connect to DB
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")

    # Truncate table before inserting
        cur.execute(f"TRUNCATE TABLE staging.stg_user_credit_card")
        conn.commit()
        logging.info(f"Truncated table staging.stg_user_credit_card before ingestion")

    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    try:
        # Load pickle file
        df = pd.read_pickle(FILE_PATH)

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Required columns
        required_cols = ["user_id", "name", "credit_card_number", "issuing_bank"]

        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' missing in pickle file")

            df[col] = df[col].astype(str)  # Convert all to strings for staging

        # Add metadata columns
        df["source_filename"] = FILE_PATH
        df["ingestion_date"] = datetime.now()

        insert_cols = required_cols + ["source_filename", "ingestion_date"]
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

        # Batch insert
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            execute_values(
                cur,
                f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
                batch
            )
            conn.commit()
            logging.info(f"Inserted rows {i+1} to {i+len(batch)}")

        logging.info(f"Ingestion complete: {len(data_tuples)} rows inserted.")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_user_credit_card()
