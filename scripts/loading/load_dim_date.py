import logging
import os
import sys

# Add the scripts directory to the python path so we can import database_connection
sys.path.append('/opt/airflow/scripts')

from execute_sql_file import execute_sql_file

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SQL_FILE = "/opt/airflow/sql/load/load_dim_date.sql"

def run_loading():
    """Execute the load_dim_date.sql load script"""
    try:
        success = execute_sql_file(SQL_FILE)
        if success:
            logging.info("Successfully executed: load_dim_date.sql")
        else:
            logging.error(f"Failed to execute load_dim_date.sql")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Error executing load_dim_date.sql: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_loading()
