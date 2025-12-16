import logging
from database_connection import get_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SQL_FILE = "/opt/airflow/sql/load/load_fact_order.sql"

def run_loading():
    """Execute the load_fact_order.sql loading script"""
    conn = get_connection()
    if not conn:
        logging.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        logging.info(f"Executing loading script: load_fact_order.sql")
        
        with open(SQL_FILE, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        rows_affected = cursor.rowcount
        conn.commit()
        
        logging.info(f"Successfully executed: load_fact_order.sql (rows affected: {rows_affected})")
        cursor.close()
        return True
        
    except Exception as e:
        logging.error(f"Error executing load_fact_order.sql: {str(e)}")
        conn.rollback()
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_loading()
