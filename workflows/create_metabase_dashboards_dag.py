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
    'start_date': datetime(2025, 12, 18),
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
setup_metabase_task = PythonOperator(
    task_id='setup_metabase',
    python_callable=run_python_script,
    op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/setup_metabase.py'},
    dag=dag,
)

refresh_metabase_task = PythonOperator(
    task_id='refresh_metabase',
    python_callable=run_python_script,
    op_kwargs={'script_path': '/opt/airflow/dashboard/scripts/refresh_metabase.py'},
    dag=dag,
)

# =============================================================================
# DASHBOARD CREATION TASKS (12)
# =============================================================================
exec_dash = PythonOperator(task_id='create_executive_overview_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_executive_overview', 'function_name': 'create_executive_overview_dashboard'}, dag=dag)
camp_dash = PythonOperator(task_id='create_campaign_performance_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_campaign_performance', 'function_name': 'create_campaign_performance_dashboard'}, dag=dag)
cust_dash = PythonOperator(task_id='create_customer_analytics_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_customer_analytics', 'function_name': 'create_customer_analytics_dashboard'}, dag=dag)
geo_dash = PythonOperator(task_id='create_geographic_performance_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_geographic_performance', 'function_name': 'create_geographic_performance_dashboard'}, dag=dag)
merch_dash = PythonOperator(task_id='create_merchant_performance_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_merchant_performance', 'function_name': 'create_merchant_performance_dashboard'}, dag=dag)
prod_dash = PythonOperator(task_id='create_product_performance_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_product_performance', 'function_name': 'create_product_performance_dashboard'}, dag=dag)
staff_dash = PythonOperator(task_id='create_staff_operations_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_staff_operations', 'function_name': 'create_staff_operations_dashboard'}, dag=dag)
deliv_dash = PythonOperator(task_id='create_delivery_logistics_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_delivery_logistics', 'function_name': 'create_delivery_logistics_dashboard'}, dag=dag)
time_dash = PythonOperator(task_id='create_time_based_analysis_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_time_based_analysis', 'function_name': 'create_time_based_analysis_dashboard'}, dag=dag)
basket_dash = PythonOperator(task_id='create_market_basket_analysis_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_market_basket_analysis', 'function_name': 'create_market_basket_analysis_dashboard'}, dag=dag)
seg_dash = PythonOperator(task_id='create_customer_segmentation_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_customer_segmentation', 'function_name': 'create_customer_segmentation_dashboard'}, dag=dag)
roi_dash = PythonOperator(task_id='create_campaign_roi_dashboard', python_callable=run_script, op_kwargs={'script_name': 'create_campaign_roi', 'function_name': 'create_campaign_roi_dashboard'}, dag=dag)

# =============================================================================
# CARD CREATION TASKS (12)
# =============================================================================
exec_cards = PythonOperator(task_id='create_executive_overview_cards', python_callable=run_script, op_kwargs={'script_name': 'executive_overview_cards', 'function_name': 'create_executive_overview_cards'}, dag=dag)
camp_cards = PythonOperator(task_id='create_campaign_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'campaign_performance_cards', 'function_name': 'create_campaign_performance_cards'}, dag=dag)
cust_cards = PythonOperator(task_id='create_customer_analytics_cards', python_callable=run_script, op_kwargs={'script_name': 'customer_analytics_cards', 'function_name': 'create_customer_analytics_cards'}, dag=dag)
geo_cards = PythonOperator(task_id='create_geographic_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'geographic_performance_cards', 'function_name': 'create_geographic_performance_cards'}, dag=dag)
merch_cards = PythonOperator(task_id='create_merchant_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'merchant_performance_cards', 'function_name': 'create_merchant_performance_cards'}, dag=dag)
prod_cards = PythonOperator(task_id='create_product_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'product_performance_cards', 'function_name': 'create_product_performance_cards'}, dag=dag)
staff_cards = PythonOperator(task_id='create_staff_operations_cards', python_callable=run_script, op_kwargs={'script_name': 'staff_operations_cards', 'function_name': 'create_staff_operations_cards'}, dag=dag)
deliv_cards = PythonOperator(task_id='create_delivery_logistics_cards', python_callable=run_script, op_kwargs={'script_name': 'delivery_logistics_cards', 'function_name': 'create_delivery_logistics_cards'}, dag=dag)
time_cards = PythonOperator(task_id='create_time_based_analysis_cards', python_callable=run_script, op_kwargs={'script_name': 'time_based_analysis_cards', 'function_name': 'create_time_based_analysis_cards'}, dag=dag)
basket_cards = PythonOperator(task_id='create_market_basket_analysis_cards', python_callable=run_script, op_kwargs={'script_name': 'market_basket_analysis_cards', 'function_name': 'create_market_basket_analysis_cards'}, dag=dag)
seg_cards = PythonOperator(task_id='create_customer_segmentation_cards', python_callable=run_script, op_kwargs={'script_name': 'customer_segmentation_cards', 'function_name': 'create_customer_segmentation_cards'}, dag=dag)
roi_cards = PythonOperator(task_id='create_campaign_roi_cards', python_callable=run_script, op_kwargs={'script_name': 'campaign_roi_cards', 'function_name': 'create_campaign_roi_cards'}, dag=dag)

# =============================================================================
# CARD INSERTION TASKS (12)
# =============================================================================
exec_insert = PythonOperator(task_id='insert_executive_overview_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_executive_overview', 'function_name': 'build_executive_dashboard'}, dag=dag)
camp_insert = PythonOperator(task_id='insert_campaign_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_campaign_performance', 'function_name': 'insert_campaign_performance_cards'}, dag=dag)
cust_insert = PythonOperator(task_id='insert_customer_analytics_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_customer_analytics', 'function_name': 'insert_customer_analytics_cards'}, dag=dag)
geo_insert = PythonOperator(task_id='insert_geographic_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_geographic_performance', 'function_name': 'insert_geographic_performance_cards'}, dag=dag)
merch_insert = PythonOperator(task_id='insert_merchant_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_merchant_performance', 'function_name': 'insert_merchant_performance_cards'}, dag=dag)
prod_insert = PythonOperator(task_id='insert_product_performance_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_product_performance', 'function_name': 'insert_product_performance_cards'}, dag=dag)
staff_insert = PythonOperator(task_id='insert_staff_operations_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_staff_operations', 'function_name': 'insert_staff_operations_cards'}, dag=dag)
deliv_insert = PythonOperator(task_id='insert_delivery_logistics_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_delivery_logistics', 'function_name': 'insert_delivery_logistics_cards'}, dag=dag)
time_insert = PythonOperator(task_id='insert_time_based_analysis_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_time_based_analysis', 'function_name': 'insert_time_based_analysis_cards'}, dag=dag)
basket_insert = PythonOperator(task_id='insert_market_basket_analysis_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_market_basket_analysis', 'function_name': 'insert_market_basket_analysis_cards'}, dag=dag)
seg_insert = PythonOperator(task_id='insert_customer_segmentation_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_customer_segmentation', 'function_name': 'insert_customer_segmentation_cards'}, dag=dag)
roi_insert = PythonOperator(task_id='insert_campaign_roi_cards', python_callable=run_script, op_kwargs={'script_name': 'insert_campaign_roi', 'function_name': 'insert_campaign_roi_cards'}, dag=dag)

# =============================================================================
# DEPENDENCIES
# =============================================================================
# Setup sequence
setup_metabase_task >> refresh_metabase_task

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
