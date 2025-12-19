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

def create_customer_segmentation_cards():
    logging.info("Creating Customer Segmentation Deep Dive cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Customer Segments",
            "sql": """SELECT COUNT(DISTINCT CONCAT(user_type, '-', job_level)) as "Total Segments"
FROM warehouse.dimcustomer c
JOIN warehouse.factorder f ON c.customer_key = f.customer_key
WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Segment Diversity Index",
            "sql": """WITH segment_counts AS (
  SELECT 
    CONCAT(c.user_type, '-', c.job_level) as segment,
    COUNT(DISTINCT c.customer_key) as customer_count
  FROM warehouse.dimcustomer c
  JOIN warehouse.factorder f ON c.customer_key = f.customer_key
  WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
  GROUP BY CONCAT(c.user_type, '-', c.job_level)
)
SELECT ROUND(
  (SUM(customer_count * customer_count)::NUMERIC / 
   (SUM(customer_count) * SUM(customer_count))::NUMERIC),
  4
) as "Diversity Index"
FROM segment_counts""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Top Segment Revenue",
            "sql": """WITH segment_revenue AS (
  SELECT 
    CONCAT(c.user_type, '-', c.job_level) as segment,
    SUM(f.net_order_amount) as revenue
  FROM warehouse.factorder f
  JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
  WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
  GROUP BY CONCAT(c.user_type, '-', c.job_level)
)
SELECT revenue as "Top Segment Revenue"
FROM segment_revenue
ORDER BY revenue DESC
LIMIT 1""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Top Segment Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Top Segment Orders",
            "sql": """WITH segment_orders AS (
  SELECT 
    CONCAT(c.user_type, '-', c.job_level) as segment,
    COUNT(f.order_id) as orders
  FROM warehouse.factorder f
  JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
  WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
  GROUP BY CONCAT(c.user_type, '-', c.job_level)
)
SELECT orders as "Top Segment Orders"
FROM segment_orders
ORDER BY orders DESC
LIMIT 1""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Segment AOV Ranking",
            "sql": """SELECT 
  CONCAT(c.user_type, ' - ', c.job_level) as "Segment",
  ROUND(AVG(f.net_order_amount), 2) as "AOV",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
GROUP BY c.user_type, c.job_level
HAVING COUNT(f.order_id) >= 10
ORDER BY "AOV" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "column_settings": {
                    "[\"name\",\"AOV\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Segment Campaign Usage Rate",
            "sql": """SELECT 
  CONCAT(c.user_type, ' - ', c.job_level) as "Segment",
  ROUND(
    (COUNT(CASE WHEN f.availed_flag = TRUE THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
    2
  ) as "Campaign Usage %",
  COUNT(f.order_id) as "Total Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
GROUP BY c.user_type, c.job_level
HAVING COUNT(f.order_id) >= 10
ORDER BY "Campaign Usage %" DESC
LIMIT 15""",
            "viz": {
                "display": "bar",
                "column_settings": {
                    "[\"name\",\"Campaign Usage %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Segment Repeat Purchase Rate",
            "sql": """WITH customer_orders AS (
  SELECT 
    CONCAT(c.user_type, ' - ', c.job_level) as segment,
    f.customer_key,
    COUNT(f.order_id) as order_count
  FROM warehouse.factorder f
  JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
  WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
  GROUP BY c.user_type, c.job_level, f.customer_key
)
SELECT 
  segment as "Segment",
  ROUND(
    (COUNT(CASE WHEN order_count > 1 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
    2
  ) as "Repeat Rate %",
  COUNT(*) as "Customers"
FROM customer_orders
GROUP BY segment
HAVING COUNT(*) >= 10
ORDER BY "Repeat Rate %" DESC""",
            "viz": {
                "display": "bar",
                "column_settings": {
                    "[\"name\",\"Repeat Rate %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Segment Matrix - Revenue vs Orders",
            "sql": """SELECT 
  CONCAT(c.user_type, ' - ', c.job_level) as "Segment",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders",
  ROUND(AVG(f.net_order_amount), 2) as "AOV"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.user_type IS NOT NULL AND c.job_level IS NOT NULL
GROUP BY c.user_type, c.job_level
HAVING COUNT(f.order_id) >= 10
ORDER BY "Revenue" DESC
LIMIT 10""",
            "viz": {
                "display": "scatter",
                "graph.dimensions": ["Orders", "Revenue"],
                "graph.metrics": ["Revenue"],
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
                    }
                }
            }
        },
        {
            "name": "Age and Gender Analysis",
            "sql": """SELECT 
  CASE 
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) < 25 THEN 'Under 25'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 25 AND 34 THEN '25-34'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 35 AND 44 THEN '35-44'
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 45 AND 54 THEN '45-54'
    ELSE '55+'
  END as "Age Group",
  CASE 
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) < 25 THEN 1
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 25 AND 34 THEN 2
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 35 AND 44 THEN 3
    WHEN EXTRACT(YEAR FROM AGE(c.birthdate)) BETWEEN 45 AND 54 THEN 4
    ELSE 5
  END as age_order,
  c.gender as "Gender",
  COUNT(f.order_id) as "Orders",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
WHERE c.birthdate IS NOT NULL AND c.gender IS NOT NULL
GROUP BY "Age Group", age_order, c.gender
ORDER BY age_order, c.gender""",
            "viz": {
                "display": "bar",
                "graph.dimensions": ["Age Group"],
                "graph.metrics": ["Revenue"],
                "series_settings": {"Gender": {"color": "#509EE3"}},
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
    logging.info("CUSTOMER SEGMENTATION DEEP DIVE CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_customer_segmentation_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Customer Segmentation Deep Dive cards: {e}")
        exit(1)
