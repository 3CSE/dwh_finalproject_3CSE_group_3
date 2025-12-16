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

SQL_FILE = "/opt/airflow/sql/load/load_dim_staff.sql"

def run_loading():
<<<<<<< HEAD
    """Execute the load_dim_staff.sql loading script"""
    conn = get_connection()
    if not conn:
        logging.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        logging.info(f"Executing loading script: load_dim_staff.sql")
        
        with open(SQL_FILE, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        rows_affected = cursor.rowcount
        conn.commit()
        
        logging.info(f"Successfully executed: load_dim_staff.sql (rows affected: {rows_affected})")
        cursor.close()
        return True
        
    except Exception as e:
        logging.error(f"Error executing load_dim_staff.sql: {str(e)}")
        conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()
=======
    """Execute the load_dim_staff.sql load script"""
    try:
        success = execute_sql_file(SQL_FILE)
        if success:
            logging.info("Successfully executed: load_dim_staff.sql")
        else:
            logging.error(f"Failed to execute load_dim_staff.sql")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"Error executing load_dim_staff.sql: {str(e)}")
        sys.exit(1)
>>>>>>> a1498287bf57e57ec34a73e6561a3d2427cb5481

if __name__ == "__main__":
    run_loading()
