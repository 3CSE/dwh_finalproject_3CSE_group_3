# Dashboard Creation Scripts

This directory contains all scripts needed to create 12 comprehensive Metabase dashboards for the ShopZada data warehouse.

## 📂 Directory Structure

```
dashboard/scripts/
├── cards/                          # Individual card creation scripts
│   ├── executive_overview_cards.py
│   ├── campaign_performance_cards.py
│   ├── customer_analytics_cards.py
│   ├── geographic_performance_cards.py
│   ├── merchant_performance_cards.py
│   ├── product_performance_cards.py
│   ├── staff_operations_cards.py
│   ├── delivery_logistics_cards.py
│   ├── time_based_analysis_cards.py
│   ├── market_basket_analysis_cards.py
│   ├── customer_segmentation_cards.py
│   └── campaign_roi_cards.py
├── pages/                          # Dashboard page creation scripts
│   └── create_executive_overview.py
├── create_all_cards.py             # Master script: Create all cards
├── create_all_dashboards.py        # Master script: Create all dashboards + insert cards
└── README.md                       # This file
```

## 🎯 Dashboard Pages

The scripts create the following 12 dashboards:

1. **Executive Overview** - High-level KPIs and business performance
2. **Campaign Performance** - Campaign effectiveness analysis
3. **Customer Analytics** - Customer segmentation and behavior
4. **Geographic Performance** - Location-based analysis
5. **Merchant Performance** - Merchant operations and delivery
6. **Product Performance** - Product sales analysis
7. **Staff & Operations** - Staff efficiency metrics
8. **Market Basket Analysis** - Cross-selling opportunities
9. **Delivery & Logistics** - Delivery performance analysis
10. **Time-Based Analysis** - Trends and seasonality
11. **Customer Segmentation Deep Dive** - Detailed segment analysis
12. **Campaign ROI & Effectiveness** - Campaign ROI metrics

Each dashboard contains 7-10 visualizations optimized for single-screen viewing without scrolling.

## 🚀 Quick Start

### Prerequisites

```bash
# Ensure Docker services are running
docker-compose up -d

# Install Python dependencies (if not already installed)
pip install requests
```

### Environment Variables

The scripts use the following environment variables (with defaults):

```bash
METABASE_URL=http://shopzada_metabase:3000
METABASE_EMAIL=admin@admin.com
METABASE_PASSWORD=admin3
```

### Running from Docker (Recommended)

If you're running inside the Airflow container:

```bash
# Enter the Airflow container
docker exec -it shopzada_airflow-webserver bash

# Navigate to dashboard scripts
cd /opt/airflow/dashboard/scripts

# Run the master setup script (creates all cards and dashboards)
python create_all_cards.py          # Step 1: Create all visualization cards
python create_all_dashboards.py     # Step 2: Create dashboards and insert cards
```

### Running from Host Machine

If running from your local machine:

```bash
# Navigate to dashboard scripts directory
cd "c:\Users\NATHANIEL JOSEPH\PythonVenv\DWH FInal Project\dwh_finalproject_3CSE_group_3\dashboard\scripts"

# Run the master setup scripts
python create_all_cards.py
python create_all_dashboards.py
```

## 📊 Individual Card Scripts

Each card script creates 7-10 visualization queries for a specific dashboard.

### Run Individual Card Scripts

```bash
# Example: Create only Campaign Performance cards
python cards/campaign_performance_cards.py

# Example: Create only Product Performance cards
python cards/product_performance_cards.py
```

### Card Script Output

Each card script will:
- Login to Metabase
- Check if cards already exist (to avoid duplicates)
- Create new cards with proper SQL queries and visualizations
- Print a summary of cards created

## 🎨 Dashboard Creation

The `create_all_dashboards.py` script:
1. Creates all 12 dashboard pages
2. Retrieves card IDs by name
3. Adds cards to dashboards with optimal positioning
4. Configures card sizes for single-screen viewing

### Dashboard Card Layout

Each dashboard uses a grid system (12 columns wide):
- **KPI Cards**: Row 0, 3 columns each (4 KPIs across top)
- **Main Charts**: Rows 2-6, 6 columns each (2 charts per row)
- **Tables/Details**: Rows 8+, full width (12 columns)

## 🔧 Troubleshooting

### Cards Not Found

If you see warnings about cards not found:

```bash
# Run the card creation first
python create_all_cards.py

# Then run dashboard creation
python create_all_dashboards.py
```

### Connection Issues

If you can't connect to Metabase:

1. Check if Metabase is running:
   ```bash
   docker ps | grep metabase
   ```

2. Verify Metabase URL:
   ```bash
   curl http://localhost:3000/api/health
   ```

3. Check credentials in Metabase UI (http://localhost:3000)

### Cards Already Exist

The scripts are idempotent - they check if cards/dashboards exist before creating:
- Existing cards: Skipped with info message
- Existing dashboards: Cards are added if not already present

## 📝 Customization

### Modify Card Queries

Edit the SQL queries in individual card scripts:

```python
# Example: cards/campaign_performance_cards.py
questions = [
    {
        "name": "Campaign Revenue",
        "sql": """SELECT SUM(net_order_amount) as "Campaign Revenue"
FROM warehouse.factorder
WHERE availed_flag = TRUE""",
        "viz": {"display": "scalar", ...}
    },
    ...
]
```

### Adjust Dashboard Layout

Edit card positions in `create_all_dashboards.py`:

```python
# (card_name, row, col, size_x, size_y)
("Total Revenue", 0, 0, 3, 2),  # Row 0, Col 0, 3 cols wide, 2 rows high
```

Grid system:
- **Columns**: 0-11 (12 total)
- **Rows**: 0, 2, 4, 6, 8, 10, ...
- **Size X**: Width in columns (1-12)
- **Size Y**: Height in rows (typically 2-6)

## 📈 Card Naming Convention

All cards follow a consistent naming pattern:
- **KPIs**: Simple noun phrases (e.g., "Total Revenue", "Active Customers")
- **Charts**: Descriptive questions (e.g., "Top 5 Campaigns by Revenue")
- **Tables**: Detailed purpose (e.g., "Merchant Performance Scorecard")

## 🎯 Performance Considerations

- **Card Limits**: Top 10-15 items in bar charts
- **Time Ranges**: Last 12 months for trends
- **Aggregations**: Pre-aggregated for fast rendering
- **Filters**: Applied at query level for efficiency

## 📚 Related Documentation

- [Business Questions Document](../../DASHBOARD_BUSINESS_QUESTIONS.md) - Detailed dashboard specifications
- [Warehouse Schema](../../sql/warehouse_schema.sql) - Database structure
- [Metabase Documentation](https://www.metabase.com/docs/latest/)

## ✅ Verification

After running the scripts, verify in Metabase UI:

1. **Cards**: Navigate to Questions → Verify all cards exist
2. **Dashboards**: Navigate to Dashboards → Check all 12 dashboards
3. **Layout**: Open each dashboard → Verify no scrolling needed

## 🔄 Re-running Scripts

Safe to run multiple times:
- Cards: Will skip existing cards
- Dashboards: Will skip existing dashboards, add missing cards

To start fresh:
1. Delete dashboards/cards in Metabase UI
2. Re-run scripts

## 💡 Tips

1. **Development**: Test individual card scripts before running master script
2. **Monitoring**: Check Metabase logs for errors
3. **Performance**: Run during off-peak hours for large datasets
4. **Backup**: Export dashboards after creation using Metabase UI

## 📞 Support

For issues or questions:
1. Check Metabase logs: `docker logs shopzada_metabase`
2. Verify database connection in Metabase UI
3. Review SQL queries in card scripts for errors

---

**Created**: 2025-12-18  
**Last Updated**: 2025-12-18  
**Version**: 1.0
