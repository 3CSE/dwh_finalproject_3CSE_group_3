import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'admin@admin.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin3')

def login_to_metabase():
    response = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_EMAIL, 'password': METABASE_PASSWORD}
    )
    response.raise_for_status()
    return response.json()['id']

def get_database_id(session_token):
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/database', headers=headers)
    response.raise_for_status()
    
    for db in response.json()['data']:
        if 'shopzada' in db['name'].lower():
            return db['id']
    raise Exception("ShopZada database not found")

def create_question(session_token, database_id, name, sql, visualization_settings=None):
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
    
    response = requests.post(f'{METABASE_URL}/api/card', headers=headers, json=payload)
    response.raise_for_status()
    return response.json()['id']

def get_card_by_name(session_token, name):
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/card', headers=headers)
    response.raise_for_status()
    
    cards = response.json()
    if isinstance(cards, dict) and 'data' in cards:
        cards = cards['data']
    
    for card in cards:
        if card['name'] == name:
            return card['id']
    return None

def create_merchant_performance_cards():
    logging.info("Creating Merchant Performance cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Merchants",
            "sql": """SELECT COUNT(DISTINCT merchant_key) as "Total Merchants"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Total Merchant Revenue",
            "sql": """SELECT SUM(net_order_amount) as "Total Revenue"
FROM warehouse.factorder""",
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
            "name": "Average Merchant Orders",
            "sql": """SELECT ROUND(
  COUNT(f.order_id)::NUMERIC / COUNT(DISTINCT f.merchant_key)::NUMERIC,
  2
) as "Avg Orders per Merchant"
FROM warehouse.factorder f""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Merchant Average Delivery Delay",
            "sql": """SELECT ROUND(AVG(delay_in_days), 2) as "Avg Delivery Delay"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Top Merchants by Revenue",
            "sql": """SELECT 
  m.name as "Merchant",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
ORDER BY "Revenue" DESC
LIMIT 15""",
            "viz": {
                "display": "row",
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
            "name": "Top Merchants by Order Count",
            "sql": """SELECT 
  m.name as "Merchant",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
ORDER BY "Orders" DESC
LIMIT 15""",
            "viz": {"display": "row"}
        },
        {
            "name": "Best On-Time Delivery Merchants",
            "sql": """SELECT 
  m.name as "Merchant",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay (days)",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
HAVING COUNT(f.order_id) >= 10
ORDER BY "Avg Delay (days)" ASC
LIMIT 10""",
            "viz": {"display": "row"}
        },
        {
            "name": "Worst Delivery Performance Merchants",
            "sql": """SELECT 
  m.name as "Merchant",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay (days)",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
HAVING COUNT(f.order_id) >= 10
ORDER BY "Avg Delay (days)" DESC
LIMIT 10""",
            "viz": {"display": "row"}
        },
        {
            "name": "Merchant Performance Scorecard",
            "sql": """SELECT 
  m.name as "Merchant",
  COUNT(f.order_id) as "Orders",
  SUM(f.net_order_amount) as "Revenue",
  ROUND(AVG(f.net_order_amount), 2) as "AOV",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay",
  ROUND(
    (COUNT(CASE WHEN f.delay_in_days <= 0 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
    2
  ) as "On-Time %"
FROM warehouse.factorder f
JOIN warehouse.dimmerchant m ON f.merchant_key = m.merchant_key
GROUP BY m.name
HAVING COUNT(f.order_id) >= 5
ORDER BY "Revenue" DESC
LIMIT 20""",
            "viz": {
                "display": "table",
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"AOV\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"On-Time %\"]": {"suffix": "%"}
                }
            }
        }
    ]
    
    created_cards = []
    for q in questions:
        existing_card_id = get_card_by_name(session_token, q['name'])
        if existing_card_id:
            logging.info(f"✓ Card '{q['name']}' already exists (ID: {existing_card_id})")
            created_cards.append({"name": q['name'], "id": existing_card_id, "existed": True})
        else:
            logging.info(f"Creating card: {q['name']}")
            card_id = create_question(session_token, database_id, q['name'], q['sql'], q['viz'])
            logging.info(f"✅ Created card '{q['name']}' (ID: {card_id})")
            created_cards.append({"name": q['name'], "id": card_id, "existed": False})
    
    logging.info("\n" + "="*70)
    logging.info("MERCHANT PERFORMANCE CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_merchant_performance_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Merchant Performance cards: {e}")
        exit(1)
