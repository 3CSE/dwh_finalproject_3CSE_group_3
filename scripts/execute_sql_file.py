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
