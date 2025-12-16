import logging
from database_connection import get_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SQL_FILE = "/opt/airflow/sql/clean/clean_order.sql"

def run_cleaning():
    """Execute the clean_order.sql cleaning script"""
    conn = get_connection()
    if not conn:
        logging.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        logging.info(f"Executing cleaning script: clean_order.sql")
        
        with open(SQL_FILE, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        conn.commit()
        
        logging.info(f"Successfully executed: clean_order.sql")
        cursor.close()
        return True
        
    except Exception as e:
        logging.error(f"Error executing clean_order.sql: {str(e)}")
        conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_cleaning()
