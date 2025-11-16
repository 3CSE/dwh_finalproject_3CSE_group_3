import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from database_connection import get_connection
import logging
from bs4 import MarkupResemblesLocatorWarning
import warnings

# Load .env variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HTML_FILE = "./dataset/enterprise_department/staff_data.html"


def ingest_staff_data(
    file_path=HTML_FILE,
    table_name="staging.stg_staff",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    # Database connection
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    try:
        # Read HTML files
        with open(file_path, "r", encoding="utf-8") as f:
            df = pd.read_html(f.read())[0]

        # Drop index column if present
        if (
            df.columns[0] == 0 or
            df.columns[0] == '' or
            str(df.columns[0]).startswith("Unnamed")
        ):
            df = df.iloc[:, 1:]

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        # required column checks
        required_cols = [
            "staff_id",
            "name",
            "job_level",
            "street",
            "state",
            "city",
            "country",
            "contact_number",
            "creation_date"
        ]

        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' not found in HTML")

        # Convert date column
        df["creation_date"] = pd.to_datetime(df["creation_date"])

        # Convert all text-like fields to string
        text_cols = [
            "staff_id",
            "name",
            "job_level",
            "street",
            "state",
            "city",
            "country",
            "contact_number"
        ]
        for col in text_cols:
            df[col] = df[col].astype(str)

        # Add metadata
        df["source_filename"] = file_path
        df["ingestion_date"] = datetime.now()

        # Final order for inserting
        insert_cols = required_cols + ["source_filename", "ingestion_date"]

        # Prepare tuples
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

        logging.info(f"Ingestion complete: {len(data_tuples)} rows inserted")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_staff_data()