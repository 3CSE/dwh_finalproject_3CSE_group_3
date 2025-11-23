import pandas as pd
import os

def load_file(file_path: str) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".csv":
            return pd.read_csv(file_path)

        elif ext == ".parquet":
            return pd.read_parquet(file_path)

        elif ext in [".pickle", ".pkl"]:
            return pd.read_pickle(file_path)

        elif ext == ".json":
            return pd.read_json(file_path)

        elif ext == ".html":
            tables = pd.read_html(file_path)
            if not tables:
                raise ValueError(f"No tables found in HTML file: {file_path}")
            return tables[0]

        elif ext == ".xlsx":
            return pd.read_excel(file_path)

        else:
            raise ValueError(f"Unsupported file format: {ext}")

    except Exception as e:
        raise RuntimeError(f"Error loading file {file_path}: {e}")