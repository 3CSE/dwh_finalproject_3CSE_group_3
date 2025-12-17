import requests
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

METABASE_URL = os.getenv('METABASE_URL', 'http://shopzada_metabase:3000')
METABASE_EMAIL = os.getenv('METABASE_EMAIL', 'admin@admin.com')
METABASE_PASSWORD = os.getenv('METABASE_PASSWORD', 'admin3')

def login_to_metabase():
    response = requests.post(f'{METABASE_URL}/api/session', json={'username': METABASE_EMAIL, 'password': METABASE_PASSWORD})
    response.raise_for_status()
    return response.json()['id']

def get_dashboard_by_name(session_token, name):
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/dashboard', headers=headers)
    response.raise_for_status()
    dashboards = response.json()
    if isinstance(dashboards, dict) and 'data' in dashboards:
        dashboards = dashboards['data']
    for dashboard in dashboards:
        if dashboard['name'] == name:
            return dashboard['id']
    raise Exception(f"Dashboard '{name}' not found")

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
    raise Exception(f"Card '{name}' not found")

def insert_merchant_performance_cards():
    logging.info("Inserting Merchant Performance cards...")
    session_token = login_to_metabase()
    dashboard_id = get_dashboard_by_name(session_token, "Merchant Performance")
    
    card_layout = [
        {"name": "Total Merchants", "position": {"row": 0, "col": 0, "size_x": 6, "size_y": 4}},
        {"name": "Total Merchant Revenue", "position": {"row": 0, "col": 6, "size_x": 6, "size_y": 4}},
        {"name": "Average Merchant Orders", "position": {"row": 0, "col": 12, "size_x": 6, "size_y": 4}},
        {"name": "Merchant Average Delivery Delay", "position": {"row": 0, "col": 18, "size_x": 6, "size_y": 4}},
        {"name": "Top Merchants by Revenue", "position": {"row": 4, "col": 0, "size_x": 12, "size_y": 6}},
        {"name": "Top Merchants by Order Count", "position": {"row": 4, "col": 12, "size_x": 12, "size_y": 6}},
        {"name": "Best On-Time Delivery Merchants", "position": {"row": 10, "col": 0, "size_x": 12, "size_y": 6}},
        {"name": "Worst Delivery Performance Merchants", "position": {"row": 10, "col": 12, "size_x": 12, "size_y": 6}},
        {"name": "Merchant Performance Scorecard", "position": {"row": 16, "col": 0, "size_x": 24, "size_y": 6}},
    ]
    
    card_positions = []
    for card_info in card_layout:
        try:
            card_id = get_card_by_name(session_token, card_info['name'])
            card_positions.append((card_id, card_info['position']))
            logging.info(f"✓ Found card: {card_info['name']}")
        except:
            logging.warning(f"⚠ Card not found: {card_info['name']}")
    
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/dashboard/{dashboard_id}', headers=headers)
    dashboard_data = response.json()
    existing_dashcards = dashboard_data.get('ordered_cards', [])
    existing_card_ids = {card.get('card_id') for card in existing_dashcards}
    new_cards_to_add = [(cid, pos) for cid, pos in card_positions if cid not in existing_card_ids]
    
    if not new_cards_to_add:
        logging.info("✓ All cards already on dashboard")
        return dashboard_id
    
    dashcards = list(existing_dashcards)
    next_id = -(len(new_cards_to_add))
    for card_id, position in new_cards_to_add:
        dashcards.append({"id": next_id, "card_id": card_id, **position})
        next_id += 1
    
    response = requests.put(f'{METABASE_URL}/api/dashboard/{dashboard_id}', headers=headers, json={"dashcards": dashcards})
    response.raise_for_status()
    logging.info(f"✅ Merchant Performance dashboard populated! View at: {METABASE_URL}/dashboard/{dashboard_id}")
    return dashboard_id

if __name__ == "__main__":
    insert_merchant_performance_cards()
