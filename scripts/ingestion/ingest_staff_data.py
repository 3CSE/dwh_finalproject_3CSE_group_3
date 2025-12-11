from dotenv import load_dotenv
import logging

from scripts.file_discovery import find_valid_files
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Folder containing enterprise department staff data
DATA_DIR = "/opt/airflow/dataset/enterprise_department"

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
