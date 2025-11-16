import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from database_connection import get_connection
import logging

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CSV_FILE = "./dataset/customer_management_department/user_job.csv"

def ingest_user_job(
    file_path=CSV_FILE,
    table_name="staging.stg_user_job",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    # Connect to database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    try:
        # Load CSV
        df = pd.read_csv(file_path)

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        # Required columns
        required_cols = ["user_id", "name", "job_title", "job_level"]

        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' missing from CSV")

        # Convert everything to string (except metadata)
        text_cols = ["user_id", "name", "job_title", "job_level"]
        for col in text_cols:
            df[col] = df[col].astype(str)

        # Metadata
        df["source_filename"] = os.path.basename(file_path)
        df["ingestion_date"] = datetime.now()

        # Order of columns for DB
        insert_cols = required_cols + ["source_filename", "ingestion_date"]

        # Convert rows to tuples
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

        # Truncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        # Batch insert
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i : i + batch_size]

            execute_values(
                cur,
                f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
                batch
            )
            conn.commit()

            logging.info(f"Inserted rows {i+1} to {i+len(batch)}")

        logging.info(f"Ingestion complete: {len(data_tuples)} rows inserted")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_user_job()
