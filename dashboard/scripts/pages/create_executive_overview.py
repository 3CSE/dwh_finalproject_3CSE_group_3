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

def get_database_id(session_token):
    """Get the warehouse database ID"""
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/database', headers=headers)
    response.raise_for_status()
    
    for db in response.json()['data']:
        if 'shopzada' in db['name'].lower():
            return db['id']
    raise Exception("ShopZada database not found")

def create_question(session_token, database_id, name, sql, visualization_settings=None):
    """Create a native SQL question in Metabase"""
    headers = {'X-Metabase-Session': session_token}
    
    payload = {
        "name": name,
        "dataset_query": {
            "type": "native",
            "native": {"query": sql},
            "database": database_id
        },
        "display": "scalar" if not visualization_settings else visualization_settings.get("display", "scalar"),
        "visualization_settings": visualization_settings or {}
    }
    
    response = requests.post(
        f'{METABASE_URL}/api/card',
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    return response.json()['id']

def create_dashboard(session_token, name, description=""):
    """Create a dashboard"""
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

def add_card_to_dashboard(session_token, dashboard_id, card_id, row, col, size_x=4, size_y=4):
    """Add a card to a dashboard"""
    headers = {'X-Metabase-Session': session_token}
    
    payload = {
        "dashboard_id": dashboard_id,
        "card_id": card_id,
        "row": row,
        "col": col,
        "size_x": size_x,
        "size_y": size_y
    }
    
    response = requests.post(
        f'{METABASE_URL}/api/dashcard',
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    return response.json()

def create_dashboard_with_cards(session_token, name, description, database_id, questions):
    """Create a dashboard with all cards in one operation"""
    headers = {'X-Metabase-Session': session_token}
    
    # Create all questions first
    card_ids = []
    for q in questions:
        logging.info(f"Creating question: {q['name']}")
        card_id = create_question(
            session_token,
            database_id,
            q['name'],
            q['sql'],
            q['viz']
        )
        card_ids.append((card_id, q['position']))
    
    # Create ordered_cards structure
    ordered_cards = []
    for idx, (card_id, position) in enumerate(card_ids):
        ordered_cards.append({
            "id": idx,
            "card_id": card_id,
            "row": position['row'],
            "col": position['col'],
            "sizeX": position['size_x'],
            "sizeY": position['size_y']
        })
    
    # Create dashboard with cards
    payload = {
        "name": name,
        "description": description,
        "ordered_cards": ordered_cards
    }
    
    response = requests.post(
        f'{METABASE_URL}/api/dashboard',
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    return response.json()['id']

def build_executive_dashboard():
    """Build the Executive Overview dashboard"""
    logging.info("Starting Executive Overview dashboard creation...")
    
    # Login
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    # Get database ID
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    # Define all questions with their SQL
    questions = [
        {
            "name": "Total Revenue",
            "sql": "SELECT SUM(net_order_amount) as total_revenue FROM warehouse.factorder",
            "viz": {"display": "scalar", "scalar.field": "total_revenue"},
            "position": {"row": 0, "col": 0, "size_x": 4, "size_y": 4}
        },
        {
            "name": "Total Orders",
            "sql": "SELECT COUNT(DISTINCT order_id) as total_orders FROM warehouse.factorder",
            "viz": {"display": "scalar"},
            "position": {"row": 0, "col": 4, "size_x": 4, "size_y": 4}
        },
        {
            "name": "Average Order Value",
            "sql": "SELECT ROUND(AVG(net_order_amount), 2) as avg_order_value FROM warehouse.factorder",
            "viz": {"display": "scalar"},
            "position": {"row": 0, "col": 8, "size_x": 4, "size_y": 4}
        },
        {
            "name": "Active Customers (30 days)",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT COUNT(DISTINCT f.customer_key) as active_customers
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '30 days'""",
            "viz": {"display": "scalar"},
            "position": {"row": 0, "col": 12, "size_x": 4, "size_y": 4}
        },
        {
            "name": "Revenue Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.full_date::date as order_date,
  SUM(f.net_order_amount) as daily_revenue
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.full_date::date
ORDER BY d.full_date::date""",
            "viz": {"display": "line"},
            "position": {"row": 4, "col": 0, "size_x": 16, "size_y": 6}
        },
        {
            "name": "Order Volume Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.full_date::date as order_date,
  COUNT(f.order_id) as order_count
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.full_date::date
ORDER BY d.full_date::date""",
            "viz": {"display": "area"},
            "position": {"row": 10, "col": 0, "size_x": 16, "size_y": 6}
        },
        {
            "name": "Top 5 Campaigns by Revenue",
            "sql": """SELECT 
  c.campaign_name,
  SUM(f.net_order_amount) as total_revenue
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE c.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.campaign_name
ORDER BY total_revenue DESC
LIMIT 5""",
            "viz": {"display": "bar"},
            "position": {"row": 16, "col": 0, "size_x": 5, "size_y": 6}
        },
        {
            "name": "Top 5 Products by Revenue",
            "sql": """SELECT 
  p.product_name,
  SUM(oli.line_total_amount) as total_revenue
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5""",
            "viz": {"display": "bar"},
            "position": {"row": 16, "col": 5, "size_x": 6, "size_y": 6}
        },
        {
            "name": "Top 5 Merchants by Revenue",
            "sql": """SELECT 
  m.name as merchant_name,
  SUM(f.net_order_amount) as total_revenue
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
ORDER BY total_revenue DESC
LIMIT 5""",
            "viz": {"display": "bar"},
            "position": {"row": 16, "col": 11, "size_x": 5, "size_y": 6}
        }
    ]
    
    # Create dashboard with all cards
    dashboard_id = create_dashboard_with_cards(
        session_token,
        "Executive Overview",
        "Key performance metrics and trends for ShopZada",
        database_id,
        questions
    )
    
    logging.info(f"\n✅ Executive Overview dashboard created successfully!")
    logging.info(f"View at: {METABASE_URL}/dashboard/{dashboard_id}")
    return dashboard_id

if __name__ == "__main__":
    try:
        build_executive_dashboard()
    except Exception as e:
        logging.error(f"❌ Failed to create dashboard: {e}")
        exit(1)
