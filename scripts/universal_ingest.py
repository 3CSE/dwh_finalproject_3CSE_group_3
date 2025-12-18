import os
from datetime import datetime
import logging
import pandas as pd
from psycopg2.extras import execute_values
from scripts.file_loader import load_file
from scripts.database_connection import get_connection

"""
Universal ingestion script to handle all ingestion pipelines.

- Preserves original data types from source files.
- Adds metadata columns: source_filename, ingestion_date.
- Supports CSV, Parquet, Pickle, JSON, HTML, Excel.
- Handles batch inserts efficiently.
- Meant to be called from specific ingestion scripts.
"""

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def batch_insert(cur, table_name: str, data_tuples: list, insert_cols: list, batch_size: int = 5000):
    """Insert data in batches to avoid memory issues."""
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        execute_values(
            cur,
            f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES %s",
            batch
        )

def ingest(file_paths, table_name, required_cols, batch_size=5000, truncate=True, dtype_map=None):
    """
    Generic ingestion function with configurable row-level validation.
    
    Parameters:
    - dtype_map: dict mapping column names to expected types ('numeric', 'datetime', 'string').
    """
    if isinstance(file_paths, str):
        file_paths = [file_paths]
    if dtype_map is None:
        dtype_map = {}

    logging.info(f"Starting ingestion into table {table_name}")

    try:
        conn = get_connection()
        cur = conn.cursor()
        logging.info("Database connection successful")
    except Exception as e:
        logging.error(f"Cannot connect to database: {e}", exc_info=True)
        raise

    total_rows_inserted = 0

    try:
        if truncate:
            logging.info(f"Truncating table {table_name}")
            cur.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()

        for file_path in file_paths:
            logging.info(f"Processing file: {file_path}")

            df = load_file(file_path)
            original_row_count = len(df)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

            # --- ROW-LEVEL VALIDATION ---
            all_valid_rows_mask = pd.Series([True] * len(df), index=df.index)

            # Apply validations based on the dtype_map
            for col, expected_type in dtype_map.items():
                if col not in df.columns:
                    continue

                is_invalid = pd.Series([False] * len(df), index=df.index)
                if expected_type == 'datetime':
                    coerced_series = pd.to_datetime(df[col], errors='coerce')
                    is_invalid = coerced_series.isna()
                
                elif expected_type == 'numeric':
                    extracted_series = df[col].astype(str).str.extract(r'(\d+\.?\d*)').iloc[:, 0]
                    coerced_series = pd.to_numeric(extracted_series, errors='coerce')
                    is_invalid = coerced_series.isna()

                elif expected_type == 'string':
                    is_invalid = df[col].isna() | (df[col].astype(str).str.strip() == '')
                
                all_valid_rows_mask &= ~is_invalid
            
            invalid_df = df[~all_valid_rows_mask]
            if not invalid_df.empty:
                for index, row in invalid_df.iterrows():
                    logging.warning(f"Row {index} in {os.path.basename(file_path)} failed validation and was skipped. Data: {row.to_dict()}")

            df_clean = df[all_valid_rows_mask].copy()
            
            if df_clean.empty:
                logging.warning(f"No valid rows found in {os.path.basename(file_path)} after validation. Skipping file.")
                continue
            # --- END VALIDATION ---

            for col in required_cols:
                if col.lower() not in df_clean.columns:
                    raise KeyError(f"Required column '{col}' missing in file {file_path}")

            df_clean.loc[:, "source_filename"] = os.path.basename(file_path)
            df_clean.loc[:, "ingestion_date"] = datetime.now()

            insert_cols = [c.lower() for c in required_cols] + ["source_filename", "ingestion_date"]
            data_tuples = [tuple(row) for row in df_clean[insert_cols].to_numpy()]

            batch_insert(cur, table_name, data_tuples, insert_cols, batch_size)
            conn.commit()

            logging.info(f"{os.path.basename(file_path)}: Processed {original_row_count} rows, Inserted {len(data_tuples)} valid rows.")
            total_rows_inserted += len(data_tuples)

        logging.info(f"Ingestion complete — Total rows inserted: {total_rows_inserted}")

    except Exception as e:
        logging.error(f"A critical error occurred during ingestion: {e}", exc_info=True)
        raise

    finally:
        if 'conn' in locals() and conn:
            cur.close()
            conn.close()
            logging.info("Database connection closed")


if __name__ == "__main__":
    logging.warning("This script is a library. Use specific ingestion scripts to call 'ingest()'.")