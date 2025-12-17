import requests
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Metabase configuration
METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'admin@admin.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin3')

def wait_for_metabase():
    """Wait for Metabase to be ready"""
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(f'{METABASE_URL}/api/health', timeout=5)
            if response.status_code == 200:
                logging.info("✓ Metabase is ready")
                return True
        except:
            pass
        logging.info(f"Waiting for Metabase... ({i+1}/{max_retries})")
        time.sleep(2)
    raise Exception("Metabase did not start in time")

def check_if_setup_complete():
    """Check if Metabase setup is already complete"""
    try:
        response = requests.get(f'{METABASE_URL}/api/session/properties')
        data = response.json()
        return data.get('setup-token') is None
    except:
        return False

def create_admin_account():
    """Create the admin account if setup is needed"""
    try:
        # Get setup token
        response = requests.get(f'{METABASE_URL}/api/session/properties')
        setup_token = response.json().get('setup-token')
        
        if not setup_token:
            logging.info("✓ Admin account already exists")
            return True
        
        # Create admin account
        setup_data = {
            "token": setup_token,
            "user": {
                "first_name": "Admin",
                "last_name": "User",
                "email": METABASE_EMAIL,
                "password": METABASE_PASSWORD,
                "site_name": "ShopZada Analytics"
            },
            "prefs": {
                "site_name": "ShopZada Analytics",
                "allow_tracking": False
            }
        }
        
        response = requests.post(
            f'{METABASE_URL}/api/setup',
            json=setup_data
        )
        
        # 403 means setup is already complete - this is OK
        if response.status_code == 403:
            logging.info("✓ Setup already complete (Metabase auto-initialized)")
            return True
            
        response.raise_for_status()
        
        logging.info(f"✓ Created admin account: {METABASE_EMAIL}")
        return True
        
    except requests.exceptions.HTTPError as e:
        # If it's a 403, setup is already done - that's fine
        if e.response.status_code == 403:
            logging.info("✓ Setup already complete")
            return True
        logging.error(f"✗ Failed to create admin account: {e}")
        raise
    except Exception as e:
        logging.error(f"✗ Failed to create admin account: {e}")
        raise

def login_to_metabase():
    """Login and get session token"""
    try:
        response = requests.post(
            f'{METABASE_URL}/api/session',
            json={
                'username': METABASE_EMAIL,
                'password': METABASE_PASSWORD
            }
        )
        response.raise_for_status()
        token = response.json()['id']
        logging.info("✓ Logged in to Metabase")
        return token
    except Exception as e:
        logging.error(f"✗ Login failed: {e}")
        raise

def add_warehouse_database(session_token):
    """Add ShopZada data warehouse database connection"""
    headers = {'X-Metabase-Session': session_token}
    
    try:
        # Check if database already exists
        response = requests.get(f'{METABASE_URL}/api/database', headers=headers)
        databases = response.json()['data']
        
        for db in databases:
            if 'shopzada' in db['name'].lower():
                logging.info(f"✓ Database already exists: {db['name']}")
                return db['id']
        
        # Create database connection
        db_config = {
            "engine": "postgres",
            "name": "ShopZada Data Warehouse",
            "details": {
                "host": "postgres-dw",
                "port": 5432,
                "dbname": "shopzada_dw",
                "user": "admin",
                "password": "admin",
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": "warehouse",
                "ssl": False,
                "tunnel-enabled": False
            },
            "auto_run_queries": True,
            "is_full_sync": True,
            "schedules": {
                "metadata_sync": {
                    "schedule_type": "hourly"
                }
            }
        }
        
        response = requests.post(
            f'{METABASE_URL}/api/database',
            headers=headers,
            json=db_config
        )
        response.raise_for_status()
        
        db_id = response.json()['id']
        logging.info(f"✓ Added database connection (ID: {db_id})")
        
        # Trigger initial sync
        requests.post(
            f'{METABASE_URL}/api/database/{db_id}/sync_schema',
            headers=headers
        )
        logging.info("✓ Triggered database sync")
        
        return db_id
        
    except Exception as e:
        logging.error(f"✗ Failed to add database: {e}")
        raise

def setup_metabase():
    """Complete Metabase setup"""
    try:
        logging.info("Starting Metabase setup automation...")
        
        # Wait for Metabase to be ready
        wait_for_metabase()
        
        # Check if setup is needed
        if check_if_setup_complete():
            logging.info("✓ Setup already complete, logging in...")
        else:
            # Create admin account
            create_admin_account()
            time.sleep(2)  # Wait for account creation to complete
        
        # Login
        session_token = login_to_metabase()
        
        # Add warehouse database
        add_warehouse_database(session_token)
        
        logging.info("\n✅ Metabase setup completed successfully!")
        logging.info(f"Access Metabase at: {METABASE_URL}")
        logging.info(f"Login: {METABASE_EMAIL} / {METABASE_PASSWORD}")
        
        return True
        
    except Exception as e:
        logging.error(f"\n❌ Metabase setup failed: {e}")
        raise

if __name__ == "__main__":
    setup_metabase()
