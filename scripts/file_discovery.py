import os
import logging
from scripts.file_loader import load_file

def find_valid_files(folder_path, required_cols):
    """Scan a folder and return all files containing the required columns."""
    valid_files = []

    all_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    required_cols_lower = [c.lower() for c in required_cols]

    for file_path in all_files:
        try:
            df = load_file(file_path)

            # Normalize column names
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
            )

            # Check required columns exist
            if all(col in df.columns for col in required_cols_lower):
                valid_files.append(file_path)
            else:
                logging.warning(
                    f"Skipping {file_path}: missing required columns."
                )

        except Exception as e:
            logging.warning(
                f"Skipping {file_path}: cannot load file ({e})."
            )

    return valid_files
