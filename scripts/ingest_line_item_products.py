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

# Paths to raw files
RAW_FILES = [
    "./dataset/operations_department/line_item_data_products1.csv",
    "./dataset/operations_department/line_item_data_products2.csv",
    "./dataset/operations_department/line_item_data_products3.parquet"
]

def load_file(file_path: str) -> pd.DataFrame:
    #Load CSV or Parquet depending on file extension.
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def ingest_line_item_products(
    file_paths=RAW_FILES,
    table_name="staging.stg_line_items_products",
    batch_size=5000
):

    logging.info(f"Starting ingestion into {table_name}")

    # Connect to database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    total_rows_inserted = 0

    try:
        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")

            df = load_file(file_path)

            # Normalize column names
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

            required_cols = ["order_id", "product_name", "product_id"]

            for col in required_cols:
                if col not in df.columns:
                    raise KeyError(f"Required column '{col}' missing in file {file_path}")

            # Convert data types
            df["order_id"] = df["order_id"].astype(str)
            df["product_id"] = df["product_id"].astype(str)
            df["product_name"] = df["product_name"].astype(str)

            # Metadata
            df["source_filename"] = os.path.basename(file_path)
            df["ingestion_date"] = datetime.now()

            insert_cols = required_cols + ["source_filename", "ingestion_date"]

            # Convert to tuples
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

                logging.info(
                    f"{os.path.basename(file_path)}: Inserted rows {i+1} to {i+len(batch)}"
                )

            total_rows_inserted += len(data_tuples)

        logging.info(f"Ingestion complete — Total rows inserted: {total_rows_inserted}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_line_item_products()
