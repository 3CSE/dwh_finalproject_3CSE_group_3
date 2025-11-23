import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from scripts.database_connection import get_connection
import logging

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Path to data file
HTML_FILE = "/app/dataset/operations_department/order_delays.html"

def ingest_order_delays(
    file_path=HTML_FILE,
    table_name="staging.stg_order_delays",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    # Connect to the database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error(f"Cannot proceed without database connection: {e}")
        return

    try:
        # Read HTML table
        with open(file_path, "r", encoding="utf-8") as f:
            df = pd.read_html(f.read())[0]

        # Drop unnamed index column 
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        required_cols = ["order_id", "delay_in_days"]
        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' missing in file {file_path}")

        # Data type conversions
        df["order_id"] = df["order_id"].astype(str)
        df["delay_in_days"] = pd.to_numeric(df["delay_in_days"], errors="coerce").astype('Int64')

        # Metadata
        df["source_filename"] = os.path.basename(file_path)
        df["ingestion_date"] = datetime.now()

        insert_cols = required_cols + ["source_filename", "ingestion_date"]

        # Convert to tuples for batch insert
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

        # Truncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

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

        logging.info(f"Ingestion complete — Total rows inserted: {len(data_tuples)}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_order_delays()
