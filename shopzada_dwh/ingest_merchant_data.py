import pandas as pd
from db import get_connection
from datetime import datetime
from psycopg2.extras import execute_values

def ingest_merchant_data(file_path="merchant_data.html", table_name="staging.stg_merchant_data", batch_size=5000):
    print("Starting ingestion...")

    try:
        conn = get_connection()
        cur = conn.cursor()
        print("Database connection successful")
    except Exception as e:
        print("Failed to connect to database:", e)
        return

    try:
        # Read the first table from the HTML file
        df = pd.read_html(file_path)[0]

        # Drop the first column if it’s an index column
        if df.columns[0] == 0 or df.columns[0] == 'Unnamed: 0' or df.columns[0] == '':
            df = df.iloc[:, 1:]

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Check required columns exist
        required_cols = ['merchant_id', 'creation_date', 'name', 'street', 'state', 'city', 'country', 'contact_number']
        for col in required_cols:
            if col not in df.columns:
                raise KeyError(f"Required column '{col}' not found in HTML table")

        # Transformations
        df['creation_date'] = pd.to_datetime(df['creation_date'])  # parse timestamp
        for col in ['merchant_id', 'name', 'street', 'state', 'city', 'country', 'contact_number']:
            df[col] = df[col].astype(str)  # ensure strings

        # Add metadata
        df['source_filename'] = file_path
        df['ingestion_date'] = datetime.now()

        # Prepare for batch insert
        insert_cols = required_cols + ['source_filename', 'ingestion_date']
        data_tuples = [tuple(x) for x in df[insert_cols].to_numpy()]

        # Batch insert
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i+batch_size]
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} ({', '.join(insert_cols)})
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
    ingest_merchant_data("merchant_data.html")
