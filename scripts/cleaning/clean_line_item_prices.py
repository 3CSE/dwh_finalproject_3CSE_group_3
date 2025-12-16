import logging
import os
import sys

# Add the scripts directory to the python path so we can import database_connection
sys.path.append('/opt/airflow/scripts')

from execute_sql_file import execute_sql_file

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SQL_FILE = "/opt/airflow/sql/clean/clean_line_item_prices.sql"

def run_loading():
    """Execute the clean_line_item_prices.sql clean script"""
    try:
        success = execute_sql_file(SQL_FILE)
        if success:
            logging.info("Successfully executed: clean_line_item_prices.sql")
        else:
            logging.error(f"Failed to execute clean_line_item_prices.sql")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Error executing clean_line_item_prices.sql: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_loading()
