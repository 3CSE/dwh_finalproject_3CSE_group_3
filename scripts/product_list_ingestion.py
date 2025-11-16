import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from database_connection import get_connection
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

FILE_PATH = "./dataset/business_department/product_list.xlsx"


def ingest_product_list(
    file_path=FILE_PATH,
    table_name="staging.stg_product_list",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    # connect to database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error(f"DB connection failed: {e}")
        return

    try:
        # read file
        df = pd.read_excel(file_path, index_col=0)
        df.columns = df.columns.str.strip()


        # Add ingestion metadata
        df["source_filename"] = os.path.basename(file_path)
        df["ingestion_date"] = datetime.now()

        #Ttruncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        # Insert order
        insert_cols = [
            "product_id",
            "product_name",
            "product_type",
            "price",
            "source_filename",
            "ingestion_date"
        ]

        # Convert df rows to tuples
        rows = [tuple(x) for x in df[insert_cols].to_numpy()]

        logging.info(f"Prepared {len(rows)} rows for insertion")

        # Batch insert
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]

            execute_values(
                cur,
                f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
                batch,
            )
            conn.commit()

            logging.info(f"Inserted rows {i+1} to {i+len(batch)}")

        logging.info(f"INGESTION COMPLETE — {len(rows)} rows inserted.")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_product_list()
