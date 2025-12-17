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

def create_campaign_performance_cards():
    """Create all Campaign Performance cards/questions"""
    logging.info("Creating Campaign Performance cards...")
    
    # Login
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    # Get database ID
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    # Define all questions with their SQL
    questions = [
        {
            "name": "Campaign Revenue",
            "sql": """SELECT SUM(net_order_amount) as "Campaign Revenue"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Campaign Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Conversion Rate",
            "sql": """SELECT ROUND(
  (COUNT(CASE WHEN availed_flag = TRUE THEN 1 END)::NUMERIC / 
   COUNT(*)::NUMERIC) * 100, 
  2
) as "Conversion Rate"
FROM warehouse.factorder""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Conversion Rate\"]": {
                        "suffix": "%"
                    }
                }
            }
        },
        {
            "name": "Total Discount Cost",
            "sql": """SELECT SUM(discount_amount) as "Total Discount"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Total Discount\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Average Order Value",
            "sql": """SELECT ROUND(AVG(net_order_amount), 2) as "Campaign AOV"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Campaign AOV\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Top 5 Campaigns by Revenue",
            "sql": """SELECT 
  c.campaign_name as "Campaign",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE f.availed_flag = TRUE
GROUP BY c.campaign_name
ORDER BY "Revenue" DESC
LIMIT 5""",
            "viz": {
                "display": "row",
                "graph.x_axis.scale": "linear",
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
            "name": "Top 5 Campaigns by Order Count",
            "sql": """SELECT 
  c.campaign_name as "Campaign",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE f.availed_flag = TRUE
GROUP BY c.campaign_name
ORDER BY "Orders" DESC
LIMIT 5""",
            "viz": {
                "display": "row"
            }
        },
        {
            "name": "Discount vs Revenue Analysis",
            "sql": """SELECT 
  c.campaign_name as "Campaign",
  AVG(f.discount_amount) as "Avg Discount",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE f.availed_flag = TRUE
GROUP BY c.campaign_name
HAVING COUNT(f.order_id) >= 5
ORDER BY "Revenue" DESC""",
            "viz": {
                "display": "scatter",
                "graph.dimensions": ["Avg Discount"],
                "graph.metrics": ["Revenue"],
                "column_settings": {
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"Avg Discount\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Performance Trends",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
),
top_campaigns AS (
  SELECT c.campaign_name
  FROM warehouse.factorder f
  JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
  WHERE f.availed_flag = TRUE
  GROUP BY c.campaign_name
  ORDER BY SUM(f.net_order_amount) DESC
  LIMIT 5
)
SELECT 
  d.full_date::date as "Date",
  c.campaign_name as "Campaign",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE f.availed_flag = TRUE
  AND d.full_date >= max_date.last_date - INTERVAL '12 months'
  AND c.campaign_name IN (SELECT campaign_name FROM top_campaigns)
GROUP BY d.full_date::date, c.campaign_name
ORDER BY d.full_date::date, "Revenue" DESC""",
            "viz": {
                "display": "line",
                "graph.dimensions": ["Date"],
                "graph.metrics": ["Revenue"],
                "series_settings": {
                    "Campaign": {
                        "display": "line"
                    }
                },
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
    logging.info("CAMPAIGN PERFORMANCE CARDS CREATION SUMMARY")
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
        create_campaign_performance_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Campaign Performance cards: {e}")
        exit(1)
