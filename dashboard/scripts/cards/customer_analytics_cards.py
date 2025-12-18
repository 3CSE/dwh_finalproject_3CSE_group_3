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
    if isinstance(cards, dict) and 'data' in cards:
        cards = cards['data']
    
    for card in cards:
        if card['name'] == name:
            return card['id']
    return None

def create_customer_analytics_cards():
    """Create all Customer Analytics cards/questions"""
    logging.info("Creating Customer Analytics cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Customers",
            "sql": """SELECT COUNT(DISTINCT customer_key) as "Total Customers"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Repeat Purchase Rate (Last 30 Days)",
            "sql": """WITH customer_orders AS (
  SELECT f.customer_key, COUNT(DISTINCT f.order_id) as order_count
  FROM warehouse.factorder f
  JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
  WHERE d.full_date >= (SELECT MAX(full_date) FROM warehouse.dimdate) - INTERVAL '30 days'
  GROUP BY f.customer_key
)
SELECT COALESCE(
  ROUND(
    (COUNT(CASE WHEN order_count > 1 THEN 1 END)::NUMERIC / NULLIF(COUNT(*)::NUMERIC, 0)) * 100,
    2
  ), 0
) as "Repeat Purchase Rate"
FROM customer_orders""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Repeat Purchase Rate\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Customer Lifetime Value",
            "sql": """SELECT ROUND(
  SUM(net_order_amount) / COUNT(DISTINCT customer_key),
  2
) as "CLV"
FROM warehouse.factorder""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"CLV\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Response Rate",
            "sql": """SELECT ROUND(
  (COUNT(CASE WHEN availed_flag = TRUE THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
  2
) as "Campaign Response Rate"
FROM warehouse.factorder""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Campaign Response Rate\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Top 5 Customer Segments by Revenue",
            "sql": """SELECT 
  CONCAT(c.user_type, ' - ', c.job_level) as "Segment",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.user_type != 'Unknown' AND c.job_level != 'Unknown'
GROUP BY c.user_type, c.job_level
ORDER BY "Revenue" DESC
LIMIT 5""",
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
            "name": "Orders by Customer Segment",
            "sql": """SELECT 
  c.user_type as "Customer Type",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
GROUP BY c.user_type
ORDER BY "Orders" DESC""",
            "viz": {"display": "row"}
        },
        {
            "name": "Top 5 Age Groups by Spending",
            "sql": """SELECT 
  CASE 
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) < 25 THEN 'Under 25'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 25 AND 34 THEN '25-34'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 35 AND 44 THEN '35-44'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 45 AND 54 THEN '45-54'
    ELSE '55+'
  END as "Age Group",
  SUM(f.net_order_amount) as "Total Spending"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.birthdate IS NOT NULL
GROUP BY "Age Group"
ORDER BY "Total Spending" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "column_settings": {
                    "[\"name\",\"Total Spending\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Response by Segment",
            "sql": """SELECT 
  c.user_type as "Customer Type",
  COUNT(CASE WHEN f.availed_flag = TRUE THEN 1 END) as "Campaign Orders",
  COUNT(CASE WHEN f.availed_flag = FALSE THEN 1 END) as "Non-Campaign Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
GROUP BY c.user_type
ORDER BY "Campaign Orders" DESC""",
            "viz": {"display": "bar"}
        },
        {
            "name": "Top 5 Locations by Revenue",
            "sql": """SELECT 
  CONCAT(c.city, ', ', c.country) as "Location",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.country IS NOT NULL AND c.city IS NOT NULL
GROUP BY c.country, c.city
ORDER BY "Revenue" DESC
LIMIT 5""",
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
    logging.info("CUSTOMER ANALYTICS CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_customer_analytics_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Customer Analytics cards: {e}")
        exit(1)
