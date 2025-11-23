import os
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values
from scripts.database_connection import get_connection
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

FILE_PATH = "/app/dataset/enterprise_department/staff_data.html"


def ingest_staff(
    file_path=FILE_PATH,
    table_name="staging.stg_staff",
    batch_size=5000
):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    # connect to database
    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return

    try:
        # Read HTML file
        with open(file_path, "r", encoding="utf-8") as f:
            df = pd.read_html(f.read())[0]



        # Remove unnamed index column if present
        if df.columns[0].lower().startswith("unnamed") or df.columns[0] == "":
            df = df.iloc[:, 1:]

        df.columns = df.columns.str.strip()

        # Convert creation_date to datetime
        df["creation_date"] = pd.to_datetime(df["creation_date"])

        # Add metadata
        df["source_filename"] = os.path.basename(file_path)
        df["ingestion_date"] = datetime.now()


        # Truncate target table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        # Insert Order
        insert_cols = [
            "staff_id",
            "name",
            "job_level",
            "street",
            "state",
            "city",
            "country",
            "contact_number",
            "creation_date",
            "source_filename",
            "ingestion_date"
        ]

        # Convert DataFrame rows to tuples
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

        logging.info(f"INGESTION COMPLETE — {len(rows)} rows inserted")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_staff()
