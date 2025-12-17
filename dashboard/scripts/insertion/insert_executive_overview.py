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

def get_dashboard_by_name(session_token, name):
    """Get dashboard by name"""
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/dashboard', headers=headers)
    response.raise_for_status()
    
    dashboards = response.json()
    # Handle both list and dict with 'data' key
    if isinstance(dashboards, dict) and 'data' in dashboards:
        dashboards = dashboards['data']
    
    for dashboard in dashboards:
        if dashboard['name'] == name:
            return dashboard['id']
    raise Exception(f"Dashboard '{name}' not found")

def get_card_by_name(session_token, name):
    """Get card ID by name"""
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(f'{METABASE_URL}/api/card', headers=headers)
    response.raise_for_status()
    
    cards = response.json()
    # Handle both list and dict with 'data' key
    if isinstance(cards, dict) and 'data' in cards:
        cards = cards['data']
    
    for card in cards:
        if card['name'] == name:
            return card['id']
    raise Exception(f"Card '{name}' not found. Please run executive_overview_cards.py first.")

def build_executive_dashboard():
    """Add Executive Overview cards to the Executive Overview Dashboard"""
    logging.info("Adding Executive Overview cards to dashboard...")
    
    # Login
    session_token = login_to_metabase()
    logging.info("✓ Logged in to Metabase")
    
    # Get Executive Overview Dashboard
    dashboard_id = get_dashboard_by_name(session_token, "Executive Overview")
    logging.info(f"✓ Found Executive Overview Dashboard (ID: {dashboard_id})")
    
    # Define card names and their positions on the dashboard
    card_layout = [
        {
            "name": "Total Revenue",
            "position": {"row": 0, "col": 0, "size_x": 6, "size_y": 4}
        },
        {
            "name": "Total Orders",
            "position": {"row": 0, "col": 6, "size_x": 6, "size_y": 4}
        },
        {
            "name": "Average Order Value",
            "position": {"row": 0, "col": 12, "size_x": 6, "size_y": 4}
        },
        {
            "name": "Active Customers (30 days)",
            "position": {"row": 0, "col": 18, "size_x": 6, "size_y": 4}
        },
        {
            "name": "Revenue Over Time",
            "position": {"row": 4, "col": 0, "size_x": 24, "size_y": 6}
        },
        {
            "name": "Order Volume Over Time",
            "position": {"row": 10, "col": 0, "size_x": 24, "size_y": 6}
        },
        {
            "name": "Top 5 Campaigns by Revenue",
            "position": {"row": 16, "col": 0, "size_x": 8, "size_y": 6}
        },
        {
            "name": "Top 5 Products by Revenue",
            "position": {"row": 16, "col": 8, "size_x": 8, "size_y": 6}
        },
        {
            "name": "Top 5 Merchants by Revenue",
            "position": {"row": 16, "col": 16, "size_x": 8, "size_y": 6}
        }
    ]
    
    # Retrieve card IDs by name
    card_positions = []
    missing_cards = []
    
    for card_info in card_layout:
        try:
            card_id = get_card_by_name(session_token, card_info['name'])
            card_positions.append((card_id, card_info['position']))
            logging.info(f"✓ Found card: {card_info['name']} (ID: {card_id})")
        except Exception as e:
            missing_cards.append(card_info['name'])
            logging.warning(f"⚠ Card not found: {card_info['name']}")
    
    # Check if any cards are missing
    if missing_cards:
        logging.error(f"\n❌ Missing {len(missing_cards)} cards:")
        for card_name in missing_cards:
            logging.error(f"   - {card_name}")
        logging.error("\nPlease run the following command first:")
        logging.error("  python dashboard/scripts/cards/executive_overview_cards.py")
        raise Exception(f"Missing {len(missing_cards)} required cards")
    
    # Get the current dashboard data
    headers = {'X-Metabase-Session': session_token}
    response = requests.get(
        f'{METABASE_URL}/api/dashboard/{dashboard_id}',
        headers=headers
    )
    response.raise_for_status()
    dashboard_data = response.json()
    
    # Get existing dashboard cards
    existing_dashcards = dashboard_data.get('ordered_cards', [])
    logging.info(f"Dashboard has {len(existing_dashcards)} existing cards")
    
    # Check if cards are already on the dashboard
    existing_card_ids = {card.get('card_id') for card in existing_dashcards}
    new_cards_to_add = [(card_id, pos) for card_id, pos in card_positions if card_id not in existing_card_ids]
    
    if not new_cards_to_add:
        logging.info("✓ All cards are already on the dashboard")
        logging.info(f"View at: {METABASE_URL}/dashboard/{dashboard_id}")
        return dashboard_id
    
    # Add new cards to the dashboard
    logging.info(f"Adding {len(new_cards_to_add)} new cards to dashboard...")
    
    # Build the dashcards array with both existing and new cards
    # Each dashcard needs: id, card_id, row, col, sizeX, sizeY
    dashcards = []
    
    # Preserve existing cards
    for existing_card in existing_dashcards:
        dashcards.append(existing_card)
    
    # Add new cards with generated IDs (negative IDs will be replaced by Metabase)
    next_id = -(len(new_cards_to_add))
    for card_id, position in new_cards_to_add:
        dashcard = {
            "id": next_id,
            "card_id": card_id,
            "row": position['row'],
            "col": position['col'],
            "size_x": position['size_x'],
            "size_y": position['size_y']
        }
        dashcards.append(dashcard)
        logging.info(f"Adding card {card_id} at position ({position['row']}, {position['col']})")
        next_id += 1
    
    # Update the dashboard with the complete dashcards array
    update_payload = {
        "dashcards": dashcards
    }
    
    try:
        response = requests.put(
            f'{METABASE_URL}/api/dashboard/{dashboard_id}',
            headers=headers,
            json=update_payload
        )
        response.raise_for_status()
        result = response.json()
        
        updated_cards = result.get('dashcards', result.get('ordered_cards', []))
        logging.info(f"✓ Dashboard now has {len(updated_cards)} total cards")
        logging.info(f"✓ Successfully added {len(new_cards_to_add)} new cards")
        
    except requests.exceptions.HTTPError as e:
        logging.error(f"Error updating dashboard: {e.response.text}")
        raise
    
    logging.info(f"\n✅ Executive Overview dashboard populated successfully!")
    logging.info(f"View at: {METABASE_URL}/dashboard/{dashboard_id}")
    return dashboard_id

if __name__ == "__main__":
    try:
        build_executive_dashboard()
    except Exception as e:
        logging.error(f"❌ Failed to populate Executive Overview dashboard: {e}")
        exit(1)

