import os
from dotenv import load_dotenv
import logging

from scripts.file_loader import load_file
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing enterprise department staff data
DATA_DIR = "/app/dataset/enterprise_department"

# Table and required columns
TABLE_NAME = "staging.stg_staff"
REQUIRED_COLS = [
    "staff_id",
    "name",
    "job_level",
    "street",
    "state",
    "city",
    "country",
    "contact_number",
    "creation_date"
]
BATCH_SIZE = 5000

def find_valid_files(folder_path, required_cols):
    """Scan folder and return files containing all required columns."""
    valid_files = []

    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
                 if os.path.isfile(os.path.join(folder_path, f))]

    for file_path in all_files:
        try:
            df = load_file(file_path)
            # Normalize column names
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            if all(col in df.columns for col in required_cols):
                valid_files.append(file_path)
            else:
                logging.warning(f"Skipping {file_path}, missing required columns.")
        except Exception as e:
            logging.warning(f"Skipping {file_path}, cannot load file: {e}")

    return valid_files

def ingest_staff_data():
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
    ingest_staff_data()
