from dotenv import load_dotenv
import logging

from scripts.file_discovery import find_valid_files
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing all operations department line item files
DATA_DIR = "/opt/airflow/dataset/operations_department"

# Table and required columns
TABLE_NAME = "staging.stg_line_items_products"
REQUIRED_COLS = ["order_id", "product_name", "product_id"]
BATCH_SIZE = 5000

def ingest_line_item_products():
    valid_files = find_valid_files(DATA_DIR, REQUIRED_COLS)

    if not valid_files:
        logging.error(f"No valid files found in {DATA_DIR} for {TABLE_NAME}")
        return

    ingest(
        file_paths=valid_files,
        table_name=TABLE_NAME,
        required_cols=REQUIRED_COLS,
        batch_size=BATCH_SIZE
    )

if __name__ == "__main__":
    ingest_line_item_products()
