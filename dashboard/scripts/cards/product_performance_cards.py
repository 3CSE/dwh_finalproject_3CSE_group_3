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

def create_product_performance_cards():
    logging.info("Creating Product Performance cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Products",
            "sql": """SELECT COUNT(DISTINCT product_key) as "Total Products"
FROM warehouse.factorderlineitem""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Total Units Sold",
            "sql": """SELECT SUM(quantity) as "Total Units Sold"
FROM warehouse.factorderlineitem""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Total Product Revenue",
            "sql": """SELECT SUM(line_total_amount) as "Total Revenue"
FROM warehouse.factorderlineitem""",
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
            "name": "Average Unit Price",
            "sql": """SELECT ROUND(AVG(unit_price), 2) as "Avg Unit Price"
FROM warehouse.factorderlineitem""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Avg Unit Price\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Top Products by Revenue and Quantity",
            "sql": """SELECT 
  p.product_name as "Product",
  SUM(oli.line_total_amount) as "Revenue",
  SUM(oli.quantity) as "Units Sold"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
GROUP BY p.product_name
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
            "name": "Revenue by Product Type",
            "sql": """SELECT 
  p.product_type as "Product Type",
  SUM(oli.line_total_amount) as "Revenue",
  SUM(oli.quantity) as "Units Sold",
  COUNT(DISTINCT oli.order_id) as "Orders"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
WHERE p.product_type IS NOT NULL
GROUP BY p.product_type
ORDER BY "Revenue" DESC""",
            "viz": {
                "display": "table",
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
            "name": "Campaign Impact on Product Sales",
            "sql": """SELECT 
  p.product_type as "Product Type",
  SUM(CASE WHEN f.availed_flag = TRUE THEN oli.line_total_amount ELSE 0 END) as "Campaign Revenue",
  SUM(CASE WHEN f.availed_flag = FALSE THEN oli.line_total_amount ELSE 0 END) as "Non-Campaign Revenue"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
JOIN warehouse.factorder f ON oli.order_id = f.order_id
WHERE p.product_type IS NOT NULL
GROUP BY p.product_type
ORDER BY "Campaign Revenue" + "Non-Campaign Revenue" DESC
LIMIT 10""",
            "viz": {
                "display": "bar",
                "column_settings": {
                    "[\"name\",\"Campaign Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"Non-Campaign Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "High Volume Low Revenue Products",
            "sql": """SELECT 
  p.product_name as "Product",
  SUM(oli.quantity) as "Volume",
  SUM(oli.line_total_amount) as "Revenue",
  ROUND(AVG(oli.unit_price), 2) as "Avg Unit Price"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
GROUP BY p.product_name
HAVING SUM(oli.quantity) >= 10
ORDER BY "Volume" DESC, "Revenue" ASC
LIMIT 15""",
            "viz": {
                "display": "table",
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"Avg Unit Price\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Product Types Under Campaigns",
            "sql": """SELECT 
  p.product_type as "Product Type",
  COUNT(CASE WHEN f.availed_flag = TRUE THEN 1 END) as "Campaign Sales",
  COUNT(CASE WHEN f.availed_flag = FALSE THEN 1 END) as "Regular Sales"
FROM warehouse.factorderlineitem oli
JOIN warehouse.dimproduct p ON oli.product_key = p.product_key
JOIN warehouse.factorder f ON oli.order_id = f.order_id
WHERE p.product_type IS NOT NULL
GROUP BY p.product_type
ORDER BY "Campaign Sales" DESC""",
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
    logging.info("PRODUCT PERFORMANCE CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_product_performance_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Product Performance cards: {e}")
        exit(1)
