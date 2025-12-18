import pandas as pd
import os
import csv

def load_file(file_path: str, delimiter=None) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".csv":
            if delimiter is None:
                # Try comma first (most common), then tab, then auto-detect
                try:
                    return pd.read_csv(file_path, delimiter=',')
                except Exception:
                    try:
                        return pd.read_csv(file_path, delimiter='\t')
                    except Exception:
                        # Fallback to auto-detection
                        with open(file_path, 'r', encoding='utf-8') as f:
                            sample = f.read(8192)  # Increased sample size
                            sniffer = csv.Sniffer()
                            dialect = sniffer.sniff(sample)
                            delimiter = dialect.delimiter
                        return pd.read_csv(file_path, delimiter=delimiter)
            return pd.read_csv(file_path, delimiter=delimiter)

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
