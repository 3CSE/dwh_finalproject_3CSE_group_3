import os
from datetime import datetime
import logging
from dotenv import load_dotenv

from scripts.file_loader import load_file
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory containing campaign files
DATA_DIR = "/app/dataset/marketing_department"

# Table and required columns
TABLE_NAME = "staging.stg_campaign"
REQUIRED_COLS = ["campaign_id", "campaign_name", "campaign_description", "discount"]
BATCH_SIZE = 5000

def find_valid_files(folder_path, required_cols):
    """Scan a folder and return files that contain the required columns."""
    valid_files = []
    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    for file_path in all_files:
        try:
            df = load_file(file_path, delimiter="\t")  # CSV with tab delimiter
            # Normalize columns
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            required_cols_lower = [c.lower() for c in required_cols]

            if all(col in df.columns for col in required_cols_lower):
                valid_files.append(file_path)
            else:
                logging.warning(f"Skipping {file_path}, missing required columns.")
        except Exception as e:
            logging.warning(f"Skipping {file_path}, cannot load file: {e}")

    return valid_files

def ingest_campaign_files():
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
    ingest_campaign_files()
