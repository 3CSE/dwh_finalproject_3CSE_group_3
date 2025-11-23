import os
from dotenv import load_dotenv
import logging

from scripts.file_loader import load_file
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

# Scan a folder and return files that contain the required columns
def find_valid_files(folder_path, required_cols):
    valid_files = []

    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    for file_path in all_files:
        try:
            df = load_file(file_path)
            # Normalize columns
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            # Check if required columns exist
            if all(col.lower() in df.columns for col in required_cols):
                valid_files.append(file_path)
            else:
                logging.warning(f"Skipping {file_path}, missing required columns.")
        except Exception as e:
            logging.warning(f"Skipping {file_path}, cannot load file: {e}")

    return valid_files         

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
