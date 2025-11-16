import pandas as pd
from db import get_connection
from datetime import datetime
from psycopg2.extras import execute_values

def ingest_campaign_transactions(file_path, table_name="staging.stg_campaign_transactions", batch_size=5000):
    print("Starting ingestion...")

    try:
        conn = get_connection()
        cur = conn.cursor()
        print("Database connection successful")
    except Exception as e:
        print("Failed to connect to database:", e)
        return

    try:
        # Read CSV without specifying columns
        df = pd.read_csv(file_path, sep=',')  

        # Strip whitespace from headers
        df.columns = df.columns.str.strip()

        # Drop the first column if it’s empty
        if df.columns[0] == '' or 'Unnamed' in df.columns[0]:
            df = df.iloc[:, 1:]

        # Ensure headers are correct
        df.columns = df.columns.str.strip()

        # Transformations
        df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
        df['estimated_arrival'] = df['estimated arrival'].str.extract(r'(\d+)').astype(int)
        df['availed'] = df['availed'].astype(int)

        # Add metadata
        df['source_filename'] = file_path
        df['ingestion_date'] = datetime.now()

        # Columns to insert
        insert_cols = ['transaction_date', 'campaign_id', 'order_id', 'estimated_arrival', 'availed', 'source_filename', 'ingestion_date']
        data_tuples = [tuple(x) for x in df[insert_cols].to_numpy()]

        # Insert in batches
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i+batch_size]
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} 
                ({', '.join(insert_cols)})
                VALUES %s
                """,
                batch
            )
            conn.commit()
            print(f"Inserted rows {i+1} to {i+len(batch)}")

        print(f"Ingestion complete: {len(data_tuples)} rows inserted into {table_name}")

    except Exception as e:
        print("Error during ingestion:", e)

    finally:
        cur.close()
        conn.close()
        print("Database connection closed")


if __name__ == "__main__":
    ingest_campaign_transactions("transactional_campaign_data.csv")
