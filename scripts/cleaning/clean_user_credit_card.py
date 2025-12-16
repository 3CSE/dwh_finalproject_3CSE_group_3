import logging
<<<<<<< HEAD
from database_connection import get_connection
=======
import os
import sys

# Add the scripts directory to the python path so we can import database_connection
sys.path.append('/opt/airflow/scripts')

from execute_sql_file import execute_sql_file
>>>>>>> a1498287bf57e57ec34a73e6561a3d2427cb5481

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SQL_FILE = "/opt/airflow/sql/clean/clean_user_credit_card.sql"

<<<<<<< HEAD
def run_cleaning():
    """Execute the clean_user_credit_card.sql cleaning script"""
    conn = get_connection()
    if not conn:
        logging.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        logging.info(f"Executing cleaning script: clean_user_credit_card.sql")
        
        with open(SQL_FILE, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        conn.commit()
        
        logging.info(f"Successfully executed: clean_user_credit_card.sql")
        cursor.close()
        return True
        
    except Exception as e:
        logging.error(f"Error executing clean_user_credit_card.sql: {str(e)}")
        conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_cleaning()
=======
def run_loading():
    """Execute the clean_user_credit_card.sql clean script"""
    try:
        success = execute_sql_file(SQL_FILE)
        if success:
            logging.info("Successfully executed: clean_user_credit_card.sql")
        else:
            logging.error(f"Failed to execute clean_user_credit_card.sql")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Error executing clean_user_credit_card.sql: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_loading()
>>>>>>> a1498287bf57e57ec34a73e6561a3d2427cb5481
