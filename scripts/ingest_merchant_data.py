import os
import pandas as pd
from datetime import datetime
from psycopg2 import connect
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from database_connection import get_connection
import logging

# Load .env variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HTML_FILE = "../dataset/enterprise_department/merchant_data.html"

def ingest_merchant_data(
    file_path=HTML_FILE,
    table_name="staging.stg_merchant_data",
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
        # Read the table from HTML
        df = pd.read_html(file_path)[0]

        # Drop index column if present
        if (
            df.columns[0] == 0 or
            df.columns[0] == '' or
            str(df.columns[0]).startswith("Unnamed")
        ):
            df = df.iloc[:, 1:]

        # Normalize column names (strip & replace spaces)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

        required_cols = [
            'merchant_id', 'creation_date', 'name', 'street',
            'state', 'city', 'country', 'contact_number'
        ]

        # Validate columns
        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' not found in HTML")

        # Transformations
        df['creation_date'] = pd.to_datetime(df['creation_date'])

        # Convert text columns to string
        text_cols = [
            'merchant_id', 'name', 'street', 'state',
            'city', 'country', 'contact_number'
        ]
        for col in text_cols:
            df[col] = df[col].astype(str)

        # Metadata
        df['source_filename'] = file_path
        df['ingestion_date'] = datetime.now()

        # Order of columns for DB
        insert_cols = required_cols + ['source_filename', 'ingestion_date']

        # Convert dataframe rows to tuples
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

        logging.info(f"Ingestion complete: {len(data_tuples)} rows inserted")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_merchant_data()
