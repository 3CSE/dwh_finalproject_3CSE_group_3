from psycopg2 import connect
import os
import logging

def get_connection():
    """
    Connect to PostgreSQL using .env variables.
    """
    try:
        conn = connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            port=int(os.environ.get('DB_PORT', 5432))
        )
        return conn
    except KeyError as e:
        logging.error(f"Missing required environment variable: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise