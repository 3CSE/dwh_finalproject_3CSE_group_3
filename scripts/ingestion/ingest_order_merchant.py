import logging
from dotenv import load_dotenv
from scripts.file_discovery import find_valid_files
from scripts.universal_ingest import ingest

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Directory containing enterprise department merchant/order files
DATA_DIR = "/app/dataset/enterprise_department"

# Database table and required columns
TABLE_NAME = "staging.stg_order_merchant"
REQUIRED_COLS = ["order_id", "merchant_id", "staff_id"]
BATCH_SIZE = 5000


def ingest_order_merchant():
    # Discover all valid files containing the required columns
    valid_files = find_valid_files(
        folder_path=DATA_DIR,
        required_cols=REQUIRED_COLS
    )

    if not valid_files:
        logging.error(f"No valid files found in {DATA_DIR} for table {TABLE_NAME}")
        return

    ingest(
        file_paths=valid_files,
        table_name=TABLE_NAME,
        required_cols=REQUIRED_COLS,
        batch_size=BATCH_SIZE
    )


if __name__ == "__main__":
    ingest_order_merchant()
