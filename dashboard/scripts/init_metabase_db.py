"""
Initialize Metabase Database
Creates the metabase database if it doesn't exist
"""
import psycopg2
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def init_metabase_database():
    """Create metabase database if it doesn't exist"""
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('POSTGRES_HOST', 'shopzada_postgres'),
        'port': int(os.getenv('POSTGRES_PORT', 5432)),
        'user': os.getenv('POSTGRES_USER', 'admin'),
        'password': os.getenv('POSTGRES_PASSWORD', 'admin'),
        'database': 'postgres'  # Connect to default postgres database
    }
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        conn.autocommit = True  # Required for CREATE DATABASE
        cur = conn.cursor()
        
        # Check if metabase database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'metabase'")
        exists = cur.fetchone()
        
        if exists:
            logging.info("✓ Metabase database already exists")
        else:
            # Create metabase database
            cur.execute("CREATE DATABASE metabase")
            logging.info("✓ Created metabase database successfully")
        
        cur.close()
        conn.close()
        
        logging.info("✅ Metabase database initialization complete")
        return True
        
    except Exception as e:
        logging.error(f"❌ Error initializing metabase database: {e}")
        raise

if __name__ == "__main__":
    init_metabase_database()
