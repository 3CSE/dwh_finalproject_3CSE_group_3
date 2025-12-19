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

def create_time_based_analysis_cards():
    logging.info("Creating Time-Based Analysis cards...")
    
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    database_id = get_database_id(session_token)
    logging.info(f"✓ Found database ID: {database_id}")
    
    questions = [
        {
            "name": "Year-over-Year Growth",
            "sql": """WITH max_year AS (
  SELECT MAX(d.year) as latest_year
  FROM warehouse.dimdate d
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
),
current_year_revenue AS (
  SELECT SUM(f.net_order_amount) as revenue
  FROM warehouse.factorder f
  JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
  CROSS JOIN max_year
  WHERE d.year = max_year.latest_year
),
previous_year_revenue AS (
  SELECT SUM(f.net_order_amount) as revenue
  FROM warehouse.factorder f
  JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
  CROSS JOIN max_year
  WHERE d.year = max_year.latest_year - 1
)
SELECT COALESCE(
  ROUND(
    ((c.revenue - p.revenue) / NULLIF(p.revenue, 0)) * 100,
    2
  ),
  0
) as "YoY Growth %"
FROM current_year_revenue c, previous_year_revenue p""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"YoY Growth %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Month-over-Month Growth",
            "sql": """WITH date_range AS (
  SELECT 
    MAX(d.year) as max_year,
    MAX(d.month) as max_month
  FROM warehouse.dimdate d
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
),
current_month AS (
  SELECT SUM(f.net_order_amount) as revenue
  FROM warehouse.factorder f
  JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
  CROSS JOIN date_range
  WHERE d.year = date_range.max_year AND d.month = date_range.max_month
),
previous_month AS (
  SELECT SUM(f.net_order_amount) as revenue
  FROM warehouse.factorder f
  JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
  CROSS JOIN date_range
  WHERE (d.year = date_range.max_year AND d.month = date_range.max_month - 1)
     OR (d.year = date_range.max_year - 1 AND d.month = 12 AND date_range.max_month = 1)
)
SELECT COALESCE(
  ROUND(
    ((c.revenue - p.revenue) / NULLIF(p.revenue, 0)) * 100,
    2
  ),
  0
) as "MoM Growth %"
FROM current_month c, previous_month p""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"MoM Growth %\"]": {"suffix": "%"}
                }
            }
        },
        {
            "name": "Current Month Revenue",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT SUM(f.net_order_amount) as "Current Month Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '30 days'""",
            "viz": {
                "display": "scalar",
                "column_settings": {
                    "[\"name\",\"Current Month Revenue\"]": {
                        "number_style": "currency",
                        "currency": "PHP",
                        "currency_style": "symbol"
                    }
                }
            }
        },
        {
            "name": "Current Month Orders",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT COUNT(f.order_id) as "Current Month Orders"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '30 days'""",
            "viz": {"display": "scalar"}
        },
        {
            "name": "Revenue Trend (Last 24 Months)",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  TO_CHAR(d.full_date, 'YYYY-MM') as "Year-Month",
  d.month_name || ' ' || d.year as "Period",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '24 months'
GROUP BY TO_CHAR(d.full_date, 'YYYY-MM'), d.month_name, d.year, d.month
ORDER BY TO_CHAR(d.full_date, 'YYYY-MM')""",
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
            "name": "Order Volume Trend",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  d.full_date::date as "Date",
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY d.full_date::date
ORDER BY d.full_date::date""",
            "viz": {"display": "area"}
        },
        {
            "name": "Average Order Value Over Time",
            "sql": """WITH max_date AS (
  SELECT MAX(full_date) as last_date 
  FROM warehouse.dimdate d 
  JOIN warehouse.factorder f ON d.date_key = f.transaction_date_key
)
SELECT 
  TO_CHAR(d.full_date, 'YYYY-MM') as "Year-Month",
  d.month_name || ' ' || d.year as "Period",
  ROUND(AVG(f.net_order_amount), 2) as "Average Order Value"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE d.full_date >= max_date.last_date - INTERVAL '12 months'
GROUP BY TO_CHAR(d.full_date, 'YYYY-MM'), d.month_name, d.year, d.month
ORDER BY TO_CHAR(d.full_date, 'YYYY-MM')""",
            "viz": {
                "display": "line",
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
            "name": "Seasonality - Orders by Month",
            "sql": """SELECT 
  d.month_name as "Month",
  d.month as month_num,
  COUNT(f.order_id) as "Orders"
FROM warehouse.factorder f
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
GROUP BY d.month, d.month_name
ORDER BY d.month""",
            "viz": {
                "display": "bar"
            }
        },
        {
            "name": "Campaign Performance by Period",
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
  TO_CHAR(d.full_date, 'YYYY-MM') as "Period",
  c.campaign_name as "Campaign",
  SUM(f.net_order_amount) as "Revenue"
FROM warehouse.factorder f
JOIN warehouse.dimcampaign c ON f.campaign_key = c.campaign_key
JOIN warehouse.dimdate d ON f.transaction_date_key = d.date_key
CROSS JOIN max_date
WHERE f.availed_flag = TRUE
  AND d.full_date >= max_date.last_date - INTERVAL '12 months'
  AND c.campaign_name IN (SELECT campaign_name FROM top_campaigns)
GROUP BY TO_CHAR(d.full_date, 'YYYY-MM'), c.campaign_name, d.month
ORDER BY TO_CHAR(d.full_date, 'YYYY-MM')""",
            "viz": {
                "display": "line",
                "graph.dimensions": ["Period"],
                "graph.metrics": ["Revenue"],
                "graph.series_order": ["Campaign"],
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
    logging.info("TIME-BASED ANALYSIS CARDS CREATION SUMMARY")
    logging.info("="*70)
    logging.info(f"Total Cards: {len(created_cards)}")
    logging.info(f"Newly Created: {sum(1 for c in created_cards if not c['existed'])}")
    logging.info(f"Already Existed: {sum(1 for c in created_cards if c['existed'])}")
    logging.info("="*70)
    
    return created_cards

if __name__ == "__main__":
    try:
        create_time_based_analysis_cards()
    except Exception as e:
        logging.error(f"❌ Failed to create Time-Based Analysis cards: {e}")
        exit(1)
