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

def create_executive_overview_dashboard():
    """Create the Executive Overview Dashboard"""
    logging.info("Creating Executive Overview Dashboard...")
    
    # Login
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    # Check if dashboard already exists
    existing_id = get_dashboard_by_name(session_token, "Executive Overview")
    if existing_id:
        logging.info(f"✓ Executive Overview Dashboard already exists (ID: {existing_id})")
        logging.info(f"View at: {METABASE_URL}/dashboard/{existing_id}")
        return existing_id
    
    # Create new dashboard
    dashboard_id = create_dashboard(
        session_token,
        "Executive Overview",
        "High-level executive summary with key performance indicators and business metrics"
    )
    
    logging.info(f"✅ Executive Overview Dashboard created successfully!")
    logging.info(f"Dashboard ID: {dashboard_id}")
    logging.info(f"View at: {METABASE_URL}/dashboard/{dashboard_id}")
    return dashboard_id

if __name__ == "__main__":
    try:
        create_executive_overview_dashboard()
    except Exception as e:
        logging.error(f"❌ Failed to create Executive Overview Dashboard: {e}")
        exit(1)
