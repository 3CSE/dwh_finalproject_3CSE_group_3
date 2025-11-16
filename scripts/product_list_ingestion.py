import database_connection 
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def ingest():
    try:
        print("successful connection")
        conn = database_connection.get_connection()
    except Exception as e:
        print(e)
    
    file_path = "./dataset/business_department/product_list.xlsx"

    df = pd.read_excel(file_path, index_col=0)
    username = database_connection.DB_USER
    password = database_connection.DB_PASS
    host = database_connection.DB_HOST
    port = database_connection.DB_PORT
    database = database_connection.DB_NAME

    engine = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}")

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE staging.stg_product_list;"))
    print(f"Table staging.stg_product_list truncated successfully.")
    
    df["source_filename"] = os.path.basename(file_path)
    df["ingestion_date"] = datetime.now()

    df.to_sql(
        name="stg_product_list",
        schema="staging",
        con=engine,
        if_exists="append",
        index=False
    )

    print("Data loaded successfuly into the table staging.stg_product_list")

if __name__ == '__main__':
    ingest()