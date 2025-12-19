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

def create_market_basket_analysis_cards():
    logging.info("Creating Market Basket Analysis cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Average Items per Order",
            "sql": """SELECT ROUND(AVG(number_of_items), 2) as "Avg Items per Order"
FROM warehouse.factorder""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Multi-Item Orders Percentage",
            "sql": """SELECT ROUND(
  (COUNT(CASE WHEN number_of_items > 1 THEN 1 END)::NUMERIC / COUNT(*)::NUMERIC) * 100,
  2
) as "Multi-Item Orders %"
FROM warehouse.factorder""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Multi-Item Orders %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Cross-Sell Rate",
            "sql": """WITH multi_item_orders AS (
  SELECT COUNT(*) as multi_count
  FROM warehouse.factorder
  WHERE number_of_items > 1
),
single_item_orders AS (
  SELECT COUNT(*) as single_count
  FROM warehouse.factorder
  WHERE number_of_items = 1
)
SELECT ROUND(
  (m.multi_count::NUMERIC / (m.multi_count + s.single_count)::NUMERIC) * 100,
  2
) as "Cross-Sell Rate %"
FROM multi_item_orders m, single_item_orders s""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Cross-Sell Rate %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Average Basket Value",
            "sql": """SELECT ROUND(AVG(net_order_amount), 2) as "Avg Basket Value"
FROM warehouse.factorder
WHERE number_of_items > 1""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Avg Basket Value\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Product Pair Associations",
            "sql": """WITH product_pairs AS (
  SELECT 
    p1.product_type as product_a,
    p2.product_type as product_b,
    COUNT(*) as pair_count
  FROM warehouse.factorderlineitem oli1
  JOIN warehouse.factorderlineitem oli2 ON oli1.order_id = oli2.order_id
  JOIN warehouse.dimproduct p1 ON oli1.product_key = p1.product_key
  JOIN warehouse.dimproduct p2 ON oli2.product_key = p2.product_key
  WHERE oli1.product_key < oli2.product_key
    AND p1.product_type IS NOT NULL
    AND p2.product_type IS NOT NULL
  GROUP BY p1.product_type, p2.product_type
)
SELECT 
  product_a as "Product A",
  product_b as "Product B",
  pair_count as "Times Bought Together"
FROM product_pairs
WHERE pair_count >= 5
ORDER BY pair_count DESC
LIMIT 20""",
            "viz": {
                "display": "scatter",
                "graph.dimensions": ["Product A", "Product B"],
                "graph.metrics": ["Times Bought Together"]
            }
        },
        {
            "name": "Product Association Matrix",
            "sql": """WITH top_products AS (
  SELECT p.product_name, SUM(oli.line_total_amount) as revenue
  FROM warehouse.factorderlineitem oli
  JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
  GROUP BY p.product_name
  ORDER BY revenue DESC
  LIMIT 5
)
SELECT 
  p1.product_name as "Product A",
  p2.product_name as "Product B",
  COUNT(*) as "Co-occurrence"
FROM warehouse.factorderlineitem oli1
JOIN warehouse.factorderlineitem oli2 ON oli1.order_id = oli2.order_id
JOIN warehouse.dimproduct p1 ON oli1.product_key = p1.product_key
JOIN warehouse.dimproduct p2 ON oli2.product_key = p2.product_key
WHERE oli1.product_key < oli2.product_key
  AND p1.product_name IN (SELECT product_name FROM top_products)
  AND p2.product_name IN (SELECT product_name FROM top_products)
GROUP BY p1.product_name, p2.product_name
ORDER BY "Co-occurrence" DESC
LIMIT 15""",
            "viz": {"display": "table"}
        },
        {
            "name": "Basket Size Distribution",
            "sql": """WITH basket_data AS (
  SELECT 
    CASE 
      WHEN number_of_items = 1 THEN '1 item'
      WHEN number_of_items = 2 THEN '2 items'
      WHEN number_of_items = 3 THEN '3 items'
      WHEN number_of_items BETWEEN 4 AND 5 THEN '4-5 items'
      WHEN number_of_items BETWEEN 6 AND 10 THEN '6-10 items'
      ELSE '11+ items'
    END as basket_size,
    CASE 
      WHEN number_of_items = 1 THEN 1
      WHEN number_of_items = 2 THEN 2
      WHEN number_of_items = 3 THEN 3
      WHEN number_of_items BETWEEN 4 AND 5 THEN 4
      WHEN number_of_items BETWEEN 6 AND 10 THEN 5
      ELSE 6
    END as sort_order
  FROM warehouse.factorder
)
SELECT 
  basket_size as "Basket Size",
  COUNT(*) as "Order Count"
FROM basket_data
GROUP BY basket_size, sort_order
ORDER BY sort_order""",
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
    logging.info("MARKET BASKET ANALYSIS CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_market_basket_analysis_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Market Basket Analysis cards: {e}")
        exit(1)
