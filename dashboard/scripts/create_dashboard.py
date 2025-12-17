import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Metabase configuration
METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'admin@admin.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin3')

def login_to_metabase():
    """Login to Metabase and get session token"""
    response = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_EMAIL, 'password': METABASE_PASSWORD}
    )
    response.raise_for_status()
    return response.json()['id']

def get_dashboard_by_name(session_token, name):
    """Get dashboard by name, return None if not found"""
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/dashboard', headers=headers)
    response.raise_for_status()
    
    dashboards = response.json()
    # Handle both list and dict with 'data' key
    if isinstance(dashboards, dict) and 'data' in dashboards:
        dashboards = dashboards['data']
    
    for dashboard in dashboards:
        if dashboard['name'] == name:
            return dashboard['id']
    return None

def create_dashboard(session_token, name, description):
    """Create a new dashboard"""
    headers = {'X-Metabase-Session': session_token}
    
    payload = {
        "name": name,
        "description": description
    }
    
    response = requests.post(
        f'{METABASE_URL}/api/dashboard',
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    return response.json()['id']

def create_shopzada_dashboard():
    """Create the main ShopZada Dashboard"""
    logging.info("Creating ShopZada Dashboard...")
    
    # Login
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    # Check if dashboard already exists
    existing_id = get_dashboard_by_name(session_token, "ShopZada Dashboard")
    if existing_id:
        logging.info(f"✓ Dashboard already exists (ID: {existing_id})")
        logging.info(f"View at: {METABASE_URL}/dashboard/{existing_id}")
        return existing_id
    
    # Create new dashboard
    dashboard_id = create_dashboard(
        session_token,
        "ShopZada Dashboard",
        "Analytics and insights for ShopZada e-commerce platform"
    )
    
    logging.info(f"✅ ShopZada Dashboard created successfully!")
    logging.info(f"Dashboard ID: {dashboard_id}")
    logging.info(f"View at: {METABASE_URL}/dashboard/{dashboard_id}")
    return dashboard_id

if __name__ == "__main__":
    try:
        create_shopzada_dashboard()
    except Exception as e:
        logging.error(f"❌ Failed to create dashboard: {e}")
        exit(1)
