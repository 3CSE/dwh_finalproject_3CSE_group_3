import logging
from dotenv import load_dotenv

from scripts.file_discovery import find_valid_files
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Folder containing order delays files
DATA_DIR = "/app/dataset/operations_department"

# Target table and required columns
TABLE_NAME = "staging.stg_order_delays"
REQUIRED_COLS = ["order_id", "delay_in_days"]
BATCH_SIZE = 5000

def ingest_order_delays():
    """Scan folder and return files containing all required columns."""
    valid_files = find_valid_files(DATA_DIR, REQUIRED_COLS)

    if not valid_files:
        logging.error(f"No valid files found in {DATA_DIR} for table {TABLE_NAME}")
        return

    # Call the universal ingestion engine
    ingest(
        file_paths=valid_files,
        table_name=TABLE_NAME,
        required_cols=REQUIRED_COLS,
        batch_size=BATCH_SIZE,
    )

if __name__ == "__main__":
    ingest_order_delays()
