<<<<<<< HEAD
"""
Helper script to execute a single SQL file.
Used by Airflow tasks for modular execution.
"""

import os
import sys
import logging
from database_connection import get_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def execute_sql_file(sql_file_path):
    """Execute a single SQL file."""
    
    if not os.path.exists(sql_file_path):
        logger.error(f"SQL file not found: {sql_file_path}")
        return False
    
    # Get database connection
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        logger.info(f"Executing SQL file: {os.path.basename(sql_file_path)}")
        
        # Read and execute the SQL script
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        rows_affected = cursor.rowcount
        conn.commit()
        
        logger.info(f"✓ Successfully executed: {os.path.basename(sql_file_path)} (rows affected: {rows_affected})")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Error executing {os.path.basename(sql_file_path)}: {str(e)}")
        conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python execute_sql_file.py <path_to_sql_file>")
        sys.exit(1)
    
    sql_file_path = sys.argv[1]
    success = execute_sql_file(sql_file_path)
    
    if success:
        logger.info(f"SQL file executed successfully: {sql_file_path}")
        sys.exit(0)
    else:
        logger.error(f"SQL file execution failed: {sql_file_path}")
        sys.exit(1)
=======
import logging
import os
from database_connection import get_connection

def execute_sql_file(file_path):
    """
    Reads a SQL file and executes it using psycopg2
    """
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            return False

        with open(file_path, 'r') as file:
            sql_script = file.read()
            
        with conn.cursor() as cur:
            cur.execute(sql_script)
            
        conn.commit()
        logging.info(f"Successfully executed {file_path}")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error executing {file_path}: {e}")
        return False
    finally:
        if conn:
            conn.close()
>>>>>>> a1498287bf57e57ec34a73e6561a3d2427cb5481
