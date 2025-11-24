from dotenv import load_dotenv
import logging

from scripts.file_discovery import find_valid_files
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing all customer management files
DATA_DIR = "/app/dataset/customer_management_department"

# Table and required columns
TABLE_NAME = "staging.stg_user_credit_card"
REQUIRED_COLS = ["user_id", "name", "credit_card_number", "issuing_bank"]
BATCH_SIZE = 5000       

# method parameters: file_paths, table_name, required_cols, batch_size
def ingest_user_credit_card():
    valid_files = find_valid_files(DATA_DIR, REQUIRED_COLS)

    if not valid_files:
        logging.error(f"No valid files found in {DATA_DIR} for {TABLE_NAME}")

    ingest(
        file_paths= valid_files,
        table_name=TABLE_NAME,
        required_cols=REQUIRED_COLS,
        batch_size=BATCH_SIZE
    )


if __name__ == "__main__":
    ingest_user_credit_card()
