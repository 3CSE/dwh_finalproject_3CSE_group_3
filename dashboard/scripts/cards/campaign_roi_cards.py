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

def create_campaign_roi_cards():
    logging.info("Creating Campaign ROI & Effectiveness cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Total Campaign Revenue (ROI)",
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
            "name": "Total Discount Cost (ROI)",
            "sql": """SELECT SUM(discount_amount) as "Total Discount Cost"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Total Discount Cost\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Net Campaign Profit",
            "sql": """SELECT SUM(net_order_amount) - SUM(discount_amount) as "Net Campaign Profit"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Net Campaign Profit\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign ROI Percentage",
            "sql": """WITH campaign_metrics AS (
  SELECT 
    SUM(net_order_amount) as revenue,
    SUM(discount_amount) as discount
  FROM warehouse.factorder
  WHERE availed_flag = TRUE
)
SELECT ROUND(
  ((revenue - discount) / NULLIF(discount, 0)) * 100,
  2
) as "Campaign ROI %"
FROM campaign_metrics""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Campaign ROI %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Campaign AOV Ranking",
            "sql": """SELECT 
  LEFT(c.campaign_name, 30) as "Campaign",
  ROUND(AVG(f.net_order_amount), 2) as "AOV",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE f.availed_flag = TRUE
  AND c.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.campaign_name
HAVING COUNT(f.order_id) >= 5
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
            "name": "Campaign Conversion Funnel",
            "sql": """WITH campaign_metrics AS (
  SELECT 
    LEFT(c.campaign_name, 25) as campaign_name,
    COUNT(CASE WHEN f.availed_flag = TRUE THEN 1 END) as converted,
    COUNT(*) as total_exposed
  FROM warehouse.factorder f
  JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
  WHERE c.campaign_name NOT ILIKE 'unknown%'
  GROUP BY c.campaign_name
)
SELECT 
  campaign_name as "Campaign",
  total_exposed - converted as "Not Converted",
  converted as "Converted"
FROM campaign_metrics
WHERE total_exposed >= 10
ORDER BY total_exposed DESC
LIMIT 10""",
            "viz": {
                "display": "row",
                "stackable.stack_type": "stacked"
           }
        },
        {
            "name": "Discount Efficiency Analysis",
            "sql": """SELECT 
  LEFT(c.campaign_name, 25) as "Campaign",
  SUM(f.discount_amount) as "Total Discount",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE c.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.campaign_name
HAVING COUNT(f.order_id) >= 5
ORDER BY "Revenue" DESC
LIMIT 15""",
            "viz": {
                "display": "scatter",
                "graph.dimensions": ["Total Discount", "Revenue"],
                "graph.metrics": ["Revenue"],
                "column_settings": {
                    "[\"name\",\"Total Discount\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Campaign Performance Scorecard",
            "sql": """SELECT 
  LEFT(c.campaign_name, 30) as "Campaign",
  SUM(f.net_order_amount) as "Revenue",
  COUNT(f.order_id) as "Orders",
  ROUND(AVG(f.net_order_amount), 2) as "AOV",
  SUM(f.discount_amount) as "Discount Cost",
  ROUND(
    ((SUM(f.net_order_amount) - SUM(f.discount_amount)) / NULLIF(SUM(f.discount_amount), 0)) * 100,
    2
  ) as "ROI %"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
WHERE f.availed_flag = TRUE
  AND c.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.campaign_name
HAVING COUNT(f.order_id) >= 5
ORDER BY "Revenue" DESC
LIMIT 10""",
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
                    "[\"name\",\"Discount Cost\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    },
                    "[\"name\",\"ROI %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Campaign Response by Customer Segment",
            "sql": """SELECT 
  c.user_type as "Customer Type",
  LEFT(camp.campaign_name, 25) as "Campaign",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimcustomer c ON f.customer_key = c.customer_key
JOIN warehouse.dimcampaign camp ON f.campaign_key = camp.campaign_key
WHERE f.availed_flag = TRUE
  AND camp.campaign_name NOT ILIKE 'unknown%'
GROUP BY c.user_type, camp.campaign_name
HAVING COUNT(f.order_id) >= 3
ORDER BY "Orders" DESC
LIMIT 30""",
            "viz": {
                "display": "bar",
                "stackable.stack_type": "stacked"
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
    logging.info("CAMPAIGN ROI & EFFECTIVENESS CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_campaign_roi_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Campaign ROI & Effectiveness cards: {e}")
        exit(1)
