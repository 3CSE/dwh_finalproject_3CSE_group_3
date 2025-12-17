# Dashboard Automation Setup

This folder contains scripts to automatically build Metabase dashboards after ETL completion.

## 📁 Scripts

### `create_executive_dashboard.py`
Automatically creates the "Executive Overview" dashboard with:
- 4 KPI cards (Revenue, Orders, AOV, Active Customers)
- Revenue Over Time line chart
- Order Volume area chart
- Top 5 bar charts (Campaigns, Products, Merchants)

### `refresh_metabase.py`
Refreshes Metabase database metadata after ETL runs to ensure latest schema and data are visible.

## 🚀 Usage

### Manual Dashboard Creation
```bash
# After ETL completes, run:
python dashboard/scripts/create_executive_dashboard.py
```

### Automated (via Airflow)
Add to your ETL DAG as the final task:
```python
from airflow.operators.python import PythonOperator

refresh_metabase = PythonOperator(
    task_id='refresh_metabase',
    python_callable=lambda: exec(open('dashboard/scripts/refresh_metabase.py').read())
)

create_dashboards = PythonOperator(
    task_id='create_dashboards',
    python_callable=lambda: exec(open('dashboard/scripts/create_executive_dashboard.py').read())
)

# Add to DAG dependencies
load_facts >> refresh_metabase >> create_dashboards
```

## 🔧 Configuration

Set environment variables (or use defaults):
```bash
METABASE_URL=http://localhost:3000
METABASE_EMAIL=your-email@example.com
METABASE_PASSWORD=your-password
```

## ✅ What Gets Created

**Executive Overview Dashboard:**
- Total Revenue
- Total Orders  
- Average Order Value
- Active Customers (30-day window)
- Revenue trend (12 months)
- Order volume trend (12 months)
- Top 5 campaigns, products, and merchants

All queries use dynamic date handling for historical data (max date from warehouse).

## 📝 Notes

- Dashboards are created via Metabase API
- Idempotent: Safe to run multiple times (won't create duplicates if dashboard exists)
- All SQL queries sourced from dashboard guides
- Layout optimized for readability

## 🎯 Next Steps

1. Run ETL pipeline to populate data
2. Run `create_executive_dashboard.py` to build dashboard
3. Access Metabase at http://localhost:3000
4. Dashboard will be ready to use!
