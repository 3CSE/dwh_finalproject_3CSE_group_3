import csv
from db import get_connection
from datetime import datetime

def ingest_campaign_data(file_path, table_name="staging.stg_campaign"):
    print("Starting ingestion...")

    try:
        conn = get_connection()
        cur = conn.cursor()
        print("Database connection successful")
    except Exception as e:
        print("Failed to connect to database:", e)
        return

    try:
        split_data = []

        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            next(reader)

            for row in reader:
                if not row: 
                    continue
                
                if row[0].isdigit():
                    row = row[1:]

                while len(row) < 4:
                    row.append('')

                row = row[:4]
                split_data.append(row)

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
        print(f"Ingestion complete: {len(split_data)} rows inserted into {table_name}")

    except Exception as e:
        print("Error during ingestion:", e)

    finally:
        cur.close()
        conn.close()
        print("Database connection closed")


if __name__ == "__main__":
    ingest_campaign_data("campaign_data.csv")
