import requests
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'admin@admin.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin3')

def login_to_metabase():
    response = requests.post(f'{METABASE_URL}/api/session', json={'username': METABASE_EMAIL, 'password': METABASE_PASSWORD})
    response.raise_for_status()
    return response.json()['id']

def get_dashboard_by_name(session_token, name):
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
    headers = {'X-Metabase-Session': session_token}
    payload = {"name": name, "description": description}
    response = requests.post(f'{METABASE_URL}/api/dashboard', headers=headers, json=payload)
    response.raise_for_status()
    return response.json()['id']

def create_product_performance_dashboard():
    logging.info("Creating Product Performance Dashboard...")
    session_token = login_to_metabase()
    existing_id = get_dashboard_by_name(session_token, "Product Performance")
    if existing_id:
        logging.info(f"✓ Product Performance Dashboard already exists (ID: {existing_id})")
        return existing_id
    dashboard_id = create_dashboard(session_token, "Product Performance", "Product sales, inventory, and category performance analysis")
    logging.info(f"✅ Product Performance Dashboard created (ID: {dashboard_id})")
    return dashboard_id

if __name__ == "__main__":
    try:
        create_product_performance_dashboard()
    except Exception as e:
        logging.error(f"❌ Failed: {e}")
        exit(1)
