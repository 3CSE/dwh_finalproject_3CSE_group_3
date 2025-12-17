"""
Airflow DAG for creating Metabase dashboards
Full flow: Setup → Refresh → Dashboards → Cards → Insert Cards
Total: 38 tasks (2 setup + 12 dashboards + 12 cards + 12 insertions)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import subprocess
from pathlib import Path

# Add dashboard scripts to path
DASHBOARD_SCRIPTS_DIR = Path('/opt/airflow/dashboard/scripts')
CARDS_DIR = DASHBOARD_SCRIPTS_DIR / 'cards'
PAGES_DIR = DASHBOARD_SCRIPTS_DIR / 'pages'
INSERTION_DIR = DASHBOARD_SCRIPTS_DIR / 'insertion'

sys.path.insert(0, str(CARDS_DIR))
sys.path.insert(0, str(PAGES_DIR))
sys.path.insert(0, str(INSERTION_DIR))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_metabase_dashboards',
    default_args=default_args,
    description='Complete Metabase dashboard setup with 38 parallel tasks',
    schedule_interval=None,
    catchup=False,
    tags=['metabase', 'dashboards', 'setup'],
)

def run_python_script(script_path):
    result = subprocess.run(['python', script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed: {result.stderr}")
    return result.stdout

def run_script(script_name, function_name):
    module = __import__(script_name)
    create_func = getattr(module, function_name)
    return create_func()

# =============================================================================
# SETUP TASKS
# =============================================================================
# Task 0: Initialize Metabase database
init_metabase_db_task = PythonOperator(
    task_id='init_metabase_db',
    python_callable=run_python_script,
    op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/init_metabase_db.py'},
    dag=dag,
)

# Task 1: Setup Metabase admin account and add warehouse database
setup_metabase_task = PythonOperator(
    task_id='setup_metabase',
    python_callable=run_python_script,
    op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/setup_metabase.py'},
    dag=dag,
)

# Task 2: Refresh Metabase database metadata
refresh_metabase_task = PythonOperator(
    task_id='refresh_metabase',
    python_callable=run_python_script,
    op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/refresh_metabase.py'},
    dag=dag,
)

# =============================================================================
# DASHBOARD CREATION TASKS (12)
# =============================================================================
exec_dash = PythonOperator(task_id='create_executive_overview_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_executive_overview.py'}, dag=dag)
camp_dash = PythonOperator(task_id='create_campaign_performance_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_campaign_performance.py'}, dag=dag)
cust_dash = PythonOperator(task_id='create_customer_analytics_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_customer_analytics.py'}, dag=dag)
geo_dash = PythonOperator(task_id='create_geographic_performance_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_geographic_performance.py'}, dag=dag)
merch_dash = PythonOperator(task_id='create_merchant_performance_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_merchant_performance.py'}, dag=dag)
prod_dash = PythonOperator(task_id='create_product_performance_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_product_performance.py'}, dag=dag)
staff_dash = PythonOperator(task_id='create_staff_operations_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_staff_operations.py'}, dag=dag)
deliv_dash = PythonOperator(task_id='create_delivery_logistics_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_delivery_logistics.py'}, dag=dag)
time_dash = PythonOperator(task_id='create_time_based_analysis_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_time_based_analysis.py'}, dag=dag)
basket_dash = PythonOperator(task_id='create_market_basket_analysis_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_market_basket_analysis.py'}, dag=dag)
seg_dash = PythonOperator(task_id='create_customer_segmentation_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_customer_segmentation.py'}, dag=dag)
roi_dash = PythonOperator(task_id='create_campaign_roi_dashboard', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/pages/create_campaign_roi.py'}, dag=dag)

# =============================================================================
# CARD CREATION TASKS (12)
# =============================================================================
exec_cards = PythonOperator(task_id='create_executive_overview_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/executive_overview_cards.py'}, dag=dag)
camp_cards = PythonOperator(task_id='create_campaign_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/campaign_performance_cards.py'}, dag=dag)
cust_cards = PythonOperator(task_id='create_customer_analytics_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/customer_analytics_cards.py'}, dag=dag)
geo_cards = PythonOperator(task_id='create_geographic_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/geographic_performance_cards.py'}, dag=dag)
merch_cards = PythonOperator(task_id='create_merchant_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/merchant_performance_cards.py'}, dag=dag)
prod_cards = PythonOperator(task_id='create_product_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/product_performance_cards.py'}, dag=dag)
staff_cards = PythonOperator(task_id='create_staff_operations_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/staff_operations_cards.py'}, dag=dag)
deliv_cards = PythonOperator(task_id='create_delivery_logistics_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/delivery_logistics_cards.py'}, dag=dag)
time_cards = PythonOperator(task_id='create_time_based_analysis_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/time_based_analysis_cards.py'}, dag=dag)
basket_cards = PythonOperator(task_id='create_market_basket_analysis_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/market_basket_analysis_cards.py'}, dag=dag)
seg_cards = PythonOperator(task_id='create_customer_segmentation_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/customer_segmentation_cards.py'}, dag=dag)
roi_cards = PythonOperator(task_id='create_campaign_roi_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/cards/campaign_roi_cards.py'}, dag=dag)

# =============================================================================
# CARD INSERTION TASKS (12)
# =============================================================================
exec_insert = PythonOperator(task_id='insert_executive_overview_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_executive_overview.py'}, dag=dag)
camp_insert = PythonOperator(task_id='insert_campaign_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_campaign_performance.py'}, dag=dag)
cust_insert = PythonOperator(task_id='insert_customer_analytics_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_customer_analytics.py'}, dag=dag)
geo_insert = PythonOperator(task_id='insert_geographic_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_geographic_performance.py'}, dag=dag)
merch_insert = PythonOperator(task_id='insert_merchant_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_merchant_performance.py'}, dag=dag)
prod_insert = PythonOperator(task_id='insert_product_performance_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_product_performance.py'}, dag=dag)
staff_insert = PythonOperator(task_id='insert_staff_operations_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_staff_operations.py'}, dag=dag)
deliv_insert = PythonOperator(task_id='insert_delivery_logistics_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_delivery_logistics.py'}, dag=dag)
time_insert = PythonOperator(task_id='insert_time_based_analysis_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_time_based_analysis.py'}, dag=dag)
basket_insert = PythonOperator(task_id='insert_market_basket_analysis_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_market_basket_analysis.py'}, dag=dag)
seg_insert = PythonOperator(task_id='insert_customer_segmentation_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_customer_segmentation.py'}, dag=dag)
roi_insert = PythonOperator(task_id='insert_campaign_roi_cards', python_callable=run_python_script, op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/insertion/insert_campaign_roi.py'}, dag=dag)

# =============================================================================
# DEPENDENCIES
# =============================================================================
# Initialize metabase database, then setup sequence
init_metabase_db_task >> setup_metabase_task >> refresh_metabase_task

# Dashboards run after refresh (parallel)
refresh_metabase_task >> [exec_dash, camp_dash, cust_dash, geo_dash, merch_dash, prod_dash, staff_dash, deliv_dash, time_dash, basket_dash, seg_dash, roi_dash]

# Each dashboard connects to its card script (parallel)
exec_dash >> exec_cards
camp_dash >> camp_cards
cust_dash >> cust_cards
geo_dash >> geo_cards
merch_dash >> merch_cards
prod_dash >> prod_cards
staff_dash >> staff_cards
deliv_dash >> deliv_cards
time_dash >> time_cards
basket_dash >> basket_cards
seg_dash >> seg_cards
roi_dash >> roi_cards

# Each card script connects to its insertion script (parallel)
exec_cards >> exec_insert
camp_cards >> camp_insert
cust_cards >> cust_insert
geo_cards >> geo_insert
merch_cards >> merch_insert
prod_cards >> prod_insert
staff_cards >> staff_insert
deliv_cards >> deliv_insert
time_cards >> time_insert
basket_cards >> basket_insert
seg_cards >> seg_insert
roi_cards >> roi_insert
