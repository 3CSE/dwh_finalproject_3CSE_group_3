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

def create_geographic_performance_cards():
    logging.info("Creating Geographic Performance cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Countries",
            "sql": """SELECT COUNT(DISTINCT c.country) as "Total Countries"
FROM warehouse.dimcustomer c
JOIN warehouse.factorder f ON c.customer_key = f.customer_key
WHERE c.country IS NOT NULL""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Total Cities",
            "sql": """SELECT COUNT(DISTINCT c.city) as "Total Cities"
FROM warehouse.dimcustomer c
JOIN warehouse.factorder f ON c.customer_key = f.customer_key
WHERE c.city IS NOT NULL""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Average Delivery Delay",
            "sql": """SELECT ROUND(AVG(delay_in_days), 2) as "Avg Delay (days)"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "On-Time Delivery Percentage",
            "sql": """SELECT ROUND(
  (COUNT(CASE WHEN delay_in_days <= 0 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
  2
) as "On-Time Delivery %"
FROM warehouse.factorder""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"On-Time Delivery %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Revenue by Country",
            "sql": """SELECT 
  c.country as "Country",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders",
  ROUND(AVG(f.net_order_amount), 2) as "AOV"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.country IS NOT NULL
GROUP BY c.country
ORDER BY "Revenue" DESC
LIMIT 10""",
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
            "name": "Top 10 Cities by Revenue",
            "sql": """SELECT 
  c.city as "City",
  c.country as "Country",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.city IS NOT NULL
GROUP BY c.city, c.country
ORDER BY "Revenue" DESC
LIMIT 10""",
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
            "name": "Average Delay by Country",
            "sql": """SELECT 
  c.country as "Country",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay (days)",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.country IS NOT NULL
GROUP BY c.country
ORDER BY "Avg Delay (days)" DESC
LIMIT 15""",
            "viz": {"display": "table"}
        },
        {
            "name": "Delay Trends Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.year as "Year",
  d.month as "Month",
  d.month_name as "Month Name",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month""",
            "viz": {"display": "line"}
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
    logging.info("GEOGRAPHIC PERFORMANCE CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_geographic_performance_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Geographic Performance cards: {e}")
        exit(1)
