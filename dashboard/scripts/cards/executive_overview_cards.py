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

def get_card_by_name(session_token, name):
    """Get card ID by name, return None if not found"""
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/card', headers=headers)
    response.raise_for_status()
    
    cards = response.json()
    # Handle both list and dict with 'data' key
    if isinstance(cards, dict) and 'data' in cards:
        cards = cards['data']
    
    for card in cards:
        if card['name'] == name:
            return card['id']
    return None

def create_executive_overview_cards():
    """Create all Executive Overview cards/questions"""
    logging.info("Creating Executive Overview cards...")
    
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
            "sql": "SELECT SUM(net_order_amount) as \"Total Revenue\" FROM warehouse.factorder",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Total Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Total Orders",
            "sql": "SELECT COUNT(DISTINCT order_id) as \"Total Orders\" FROM warehouse.factorder",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Average Order Value",
            "sql": "SELECT ROUND(AVG(net_order_amount), 2) as \"Average Order Value\" FROM warehouse.factorder",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Average Order Value\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Active Customers (30 days)",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT COUNT(DISTINCT f.customer_key) as \"Active Customers\"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '30 days'""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Revenue Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.full_date::date as \"Date\",
  SUM(f.net_order_amount) as \"Revenue\"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.full_date::date
ORDER BY d.full_date::date""",
            "viz": {
                "display": "line",
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Order Volume Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.full_date::date as \"Date\",
  COUNT(f.order_id) as \"Orders\"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.full_date::date
ORDER BY d.full_date::date""",
            "viz": {"display": "area"}
        },
        {
            "name": "Top 5 Campaigns by Revenue",
            "sql": """SELECT 
  c.campaign_name as \"Campaign\",
  SUM(f.net_order_amount) as \"Revenue\"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE c.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.campaign_name
ORDER BY \"Revenue\" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "graph.x_axis.scale": "linear",
                "graph.x_axis.auto_range": False,
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Top 5 Products by Revenue",
            "sql": """SELECT 
  p.product_name as \"Product\",
  SUM(oli.line_total_amount) as \"Revenue\"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
GROUP BY p.product_name
ORDER BY \"Revenue\" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "graph.x_axis.scale": "linear",
                "graph.x_axis.auto_range": False,
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Top 5 Merchants by Revenue",
            "sql": """SELECT 
  m.name as \"Merchant\",
  SUM(f.net_order_amount) as \"Revenue\"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
ORDER BY \"Revenue\" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "graph.x_axis.scale": "linear",
                "graph.x_axis.auto_range": False,
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        }
    ]
    
    # Create all questions
    created_cards = []
    for q in questions:
        # Check if card already exists
        existing_card_id = get_card_by_name(session_token, q['name'])
        if existing_card_id:
            logging.info(f"✓ Card '{q['name']}' already exists (ID: {existing_card_id})")
            created_cards.append({"name": q['name'], "id": existing_card_id, "existed": True})
        else:
            logging.info(f"Creating card: {q['name']}")
            card_id = create_question(
                session_token,
                database_id,
                q['name'],
                q['sql'],
                q['viz']
            )
            logging.info(f"✅ Created card '{q['name']}' (ID: {card_id})")
            created_cards.append({"name": q['name'], "id": card_id, "existed": False})
    
    # Summary
    logging.info("\n" + "="*70)
    logging.info("EXECUTIVE OVERVIEW CARDS CREATION SUMMARY")
    logging.info("="*70)
    
    new_count = sum(1 for c in created_cards if not c["existed"])
    existing_count = sum(1 for c in created_cards if c["existed"])
    
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {new_count}")
    logging.info(f"Already Existed: {existing_count}")
    logging.info("\nCards Created:")
    
    for card in created_cards:
        status = "EXISTS" if card["existed"] else "NEW"
        logging.info(f"  [{status}] {card['name']} (ID: {card['id']})")
    
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_executive_overview_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Executive Overview cards: {e}")
        exit(1)
