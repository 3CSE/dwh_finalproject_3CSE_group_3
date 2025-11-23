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

ORDER_MERCHANT_FILES = [
    "/app/dataset/enterprise_department/order_with_merchant_data1.parquet",
    "/app/dataset/enterprise_department/order_with_merchant_data2.parquet",
    "/app/dataset/enterprise_department/order_with_merchant_data3.csv"
]

def load_merchant_file(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext == ".parquet":
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def ingest_order_merchant(
    file_paths=ORDER_MERCHANT_FILES,
    table_name="staging.stg_order_merchant",
    batch_size=5000
):
    logging.info(f"Starting ingestion into {table_name}")

    # Database connection
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception:
        logging.error("Cannot proceed without database connection")
        return

    total_rows_inserted = 0

    try:
        # Truncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")

            df = load_merchant_file(file_path)

            # Strip column whitespace
            df.columns = df.columns.str.strip()

            # Required columns
            required_cols = ["order_id", "merchant_id", "staff_id"]

            for col in required_cols:
                if col not in df.columns:
                    raise KeyError(
                        f"Required column '{col}' not found in: {file_path}"
                    )

            # Standardize data types
            for col in required_cols:
                df[col] = df[col].astype(str)

            # Metadata columns
            df["source_filename"] = os.path.basename(file_path)
            df["ingestion_date"] = datetime.now()

            insert_cols = required_cols + ["source_filename", "ingestion_date"]

            # Convert to list-of-tuples 
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

                logging.info(
                    f"{os.path.basename(file_path)} → Inserted rows {i+1} to {i+len(batch)}"
                )

            total_rows_inserted += len(data_tuples)

        logging.info(
            f"Ingestion complete — Total rows inserted: {total_rows_inserted}"
        )

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_order_merchant()
