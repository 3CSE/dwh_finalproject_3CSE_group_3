import logging
import sys
import requests
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Metabase configuration
METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'nathanieljoseph.escuro.cics@ust.edu.ph')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin123')

def login_to_metabase():
    """Login to Metabase and get session token"""
    try:
        response = requests.post(
            f'{METABASE_URL}/api/session',
            json={
                'username': METABASE_EMAIL,
                'password': METABASE_PASSWORD
            },
            timeout=30
        )
        response.raise_for_status()
        token = response.json()['id']
        logging.info("✓ Successfully logged in to Metabase")
        return token
    except Exception as e:
        logging.error(f"✗ Failed to login to Metabase: {e}")
        raise

def get_database_id(session_token, database_name='ShopZada Data Warehouse'):
    """Get the database ID for the warehouse"""
    try:
        headers = {'X-Metabase-Session': session_token}
        response = requests.get(
            f'{METABASE_URL}/api/database',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        databases = response.json()['data']
        for db in databases:
            if database_name.lower() in db['name'].lower() or 'shopzada' in db['name'].lower():
                logging.info(f"✓ Found database: {db['name']} (ID: {db['id']})")
                return db['id']
        
        # If not found by name, return first PostgreSQL database
        for db in databases:
            if db['engine'] == 'postgres':
                logging.info(f"✓ Using PostgreSQL database: {db['name']} (ID: {db['id']})")
                return db['id']
        
        raise Exception("No suitable database found")
    except Exception as e:
        logging.error(f"✗ Failed to get database ID: {e}")
        raise

def sync_database_metadata(session_token, database_id):
    """Trigger database metadata sync"""
    try:
        headers = {'X-Metabase-Session': session_token}
        response = requests.post(
            f'{METABASE_URL}/api/database/{database_id}/sync_schema',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        logging.info(f"✓ Triggered database sync for database ID: {database_id}")
        return True
    except Exception as e:
        logging.error(f"✗ Failed to sync database: {e}")
        raise

def wait_for_sync_completion(session_token, database_id, max_wait=300):
    """Wait for metadata sync to complete"""
    logging.info("Waiting for sync to complete...")
    headers = {'X-Metabase-Session': session_token}
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(
                f'{METABASE_URL}/api/database/{database_id}',
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            db_info = response.json()
            
            # Check if sync is complete
            if not db_info.get('is_sample', False):
                logging.info("✓ Database sync completed successfully")
                return True
            
            time.sleep(5)
        except Exception as e:
            logging.warning(f"Error checking sync status: {e}")
            time.sleep(5)
    
    logging.warning("Sync check timeout - continuing anyway")
    return True

def refresh_metabase():
    """Main function to refresh Metabase metadata"""
    try:
        logging.info("Starting Metabase metadata refresh...")
        
        # Login
        session_token = login_to_metabase()
        
        # Get database ID
        database_id = get_database_id(session_token)
        
        # Sync metadata
        sync_database_metadata(session_token, database_id)
        
        # Wait for completion
        wait_for_sync_completion(session_token, database_id)
        
        logging.info("✓ Metabase metadata refresh completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"✗ Failed to refresh Metabase metadata: {e}")
        sys.exit(1)

if __name__ == "__main__":
    refresh_metabase()
