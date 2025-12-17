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

def create_delivery_logistics_cards():
    logging.info("Creating Delivery & Logistics cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
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
            "name": "Average Delay Days",
            "sql": """SELECT ROUND(AVG(delay_in_days), 2) as "Avg Delay (days)"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Orders with Delays",
            "sql": """SELECT COUNT(*) as "Orders with Delays"
FROM warehouse.factorder
WHERE delay_in_days > 0""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Worst Delay",
            "sql": """SELECT MAX(delay_in_days) as "Max Delay (days)"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Delay Trend Over Time",
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
        },
        {
            "name": "Top 10 Merchants by Delay",
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
            "name": "Delay by Country",
            "sql": """SELECT 
  c.country as "Country",
  ROUND(AVG(f.delay_in_days), 2) as "Avg Delay (days)",
  COUNT(f.order_id) as "Orders"
FROM warehouse. factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.country IS NOT NULL
GROUP BY c.country
ORDER BY "Avg Delay (days)" DESC
LIMIT 15""",
            "viz": {"display": "table"}
        },
        {
            "name": "Customer Impact - Repeat Rate vs Delay",
            "sql": """WITH customer_metrics AS (
  SELECT 
    f.customer_key,
    AVG(f.delay_in_days) as avg_delay,
    COUNT(f.order_id) as order_count
  FROM warehouse.factorder f
  GROUP BY f.customer_key
  HAVING COUNT(f.order_id) >= 2
)
SELECT 
  CASE 
    WHEN avg_delay < 0 THEN 'Early'
    WHEN avg_delay = 0 THEN 'On-Time'
    WHEN avg_delay BETWEEN 1 AND 3 THEN '1-3 Days Late'
    WHEN avg_delay BETWEEN 4 AND 7 THEN '4-7 Days Late'
    ELSE '8+ Days Late'
  END as "Delay Category",
  COUNT(*) as "Repeat Customers",
  ROUND(AVG(order_count), 2) as "Avg Orders"
FROM customer_metrics
GROUP BY "Delay Category"
ORDER BY "Avg Orders" DESC""",
            "viz": {"display": "bar"}
        },
        {
            "name": "Delivery Delay Distribution",
            "sql": """SELECT 
  CASE 
    WHEN delay_in_days < 0 THEN 'Early Delivery'
    WHEN delay_in_days = 0 THEN 'On-Time'
    WHEN delay_in_days BETWEEN 1 AND 3 THEN '1-3 Days Late'
    WHEN delay_in_days BETWEEN 4 AND 7 THEN '4-7 Days Late'
    WHEN delay_in_days BETWEEN 8 AND 14 THEN '8-14 Days Late'
    ELSE '15+ Days Late'
  END as "Delay Range",
  COUNT(*) as "Order Count"
FROM warehouse.factorder
GROUP BY "Delay Range"
ORDER BY 
  CASE 
    WHEN delay_in_days < 0 THEN 1
    WHEN delay_in_days = 0 THEN 2
    WHEN delay_in_days BETWEEN 1 AND 3 THEN 3
    WHEN delay_in_days BETWEEN 4 AND 7 THEN 4
    WHEN delay_in_days BETWEEN 8 AND 14 THEN 5
    ELSE 6
  END""",
            "viz": {"display": "bar"}
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
    logging.info("DELIVERY & LOGISTICS CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_delivery_logistics_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Delivery & Logistics cards: {e}")
        exit(1)
