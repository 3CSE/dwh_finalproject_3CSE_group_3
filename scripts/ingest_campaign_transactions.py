import os
import pandas as pd
from datetime import datetime
from psycopg2 import connect
from psycopg2.extras import execute_values
from database_connection import get_connection
import logging
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CSV_FILE = "./dataset/marketing_department/transactional_campaign_data.csv"



def ingest_campaign_transactions(
    file_path=CSV_FILE,
    table_name="staging.stg_campaign_transactions",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

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
        df.columns = df.columns.str.strip()

        # Drop unnamed index column if present
        if df.columns[0].lower().startswith("unnamed") or df.columns[0] == "":
            df = df.iloc[:, 1:]

        # Normalize column names (remove spaces)
        df.columns = df.columns.str.replace(" ", "_")

        # Required transformations
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
        df["estimated_arrival"] = df["estimated_arrival"].str.extract(r"(\d+)").astype(int)
        df["availed"] = df["availed"].astype(int)

        # Add metadata
        df["source_filename"] = file_path
        df["ingestion_date"] = datetime.now()

        # Final insert order
        insert_cols = [
            "transaction_date",
            "campaign_id",
            "order_id",
            "estimated_arrival",
            "availed",
            "source_filename",
            "ingestion_date",
        ]

        # Prepare data
        data_tuples = [tuple(row) for row in df[insert_cols].to_numpy()]

        # Batch insert
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i : i + batch_size]

            execute_values(
                cur,
                f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
                batch,
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
    ingest_campaign_transactions()
