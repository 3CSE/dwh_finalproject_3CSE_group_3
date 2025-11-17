import csv
from datetime import datetime
import logging
import os
from psycopg2 import connect
from database_connection import get_connection
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CSV_FILE = "/app/dataset/marketing_department/campaign_data.csv"

def ingest_campaign(file_path=CSV_FILE, table_name="staging.stg_campaign"):
    logging.info(f"Starting ingestion for {file_path} into {table_name}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error("Cannot proceed without database connection")
        return

    try:
        split_data = []

        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            next(reader)  # skip header

            for row in reader:
                if not row:
                    continue

                # Drop leading index column if present
                if row[0].isdigit():
                    row = row[1:]

                # Ensure row has exactly 4 columns
                while len(row) < 4:
                    row.append('')

                row = row[:4]
                split_data.append(row)
                
        # Truncate table
        logging.info(f"Truncating table {table_name}")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()

        # Insert rows
        for row in split_data:
            cur.execute(
                f"""
                INSERT INTO {table_name}
                (campaign_id, campaign_name, campaign_description, discount, source_filename, ingestion_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip(), file_path, datetime.now())
            )

        conn.commit()
        logging.info(f"Ingestion complete: {len(split_data)} rows inserted into {table_name}")

    except Exception as e:
        logging.error(f"Error during ingestion: {e}")

    finally:
        cur.close()
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    ingest_campaign()
