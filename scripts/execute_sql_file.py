import logging
import os
import sys
from database_connection import get_connection

# Configure logging to output to standard out so Airflow can capture it
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_sql_file(file_path):
    """
    Reads a SQL file and executes it using psycopg2
    """
    conn = None
    try:
        if not os.path.exists(file_path):
            logging.error(f"SQL file not found: {file_path}")
            return False

        conn = get_connection()
        if conn is None:
            logging.error("Failed to connect to database")
            return False

        with open(file_path, 'r') as file:
            sql_script = file.read()
            
        with conn.cursor() as cur:
            cur.execute(sql_script)
            
        conn.commit()
        logging.info(f"Successfully executed: {os.path.basename(file_path)}")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error executing {file_path}: {e}")
        # Re-raise exception to ensure the script exits with non-zero code for Airflow
        raise e 
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Usage: python execute_sql_file.py <path_to_sql_file>")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    success = execute_sql_file(sql_file)
    
    if not success:
        sys.exit(1)
