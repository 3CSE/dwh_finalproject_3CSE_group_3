from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.utils.helpers import chain
from datetime import datetime, timedelta
import subprocess
import os
import logging

# Define generic script runner that can handle both .py scripts and .sql files (via wrapper)
def run_wrapper_safe(wrapper_script, target_file):
    if os.path.exists(wrapper_script) and os.path.exists(target_file):
        try:
            env = os.environ.copy()
            env["PYTHONPATH"] = "/opt/airflow:" + env.get("PYTHONPATH", "")
            
            # Executing: python execute_sql_file.py /path/to/script.sql
            result = subprocess.run(
                ["python", wrapper_script, target_file],
                check=True,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            logging.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running {target_file} via {wrapper_script}: {e}")
            logging.error(e.stdout if e.stdout else "No output captured")
            raise  # Raise error to fail the Airflow task
    else:
        logging.error(f"File not found: Wrapper={wrapper_script}, Target={target_file}")
        raise FileNotFoundError(f"File not found: Wrapper={wrapper_script}, Target={target_file}")

def run_script_direct(script_path):
    # For running pure python scripts like file_loader.py
    if os.path.exists(script_path):
        try:
            env = os.environ.copy()
            env["PYTHONPATH"] = "/opt/airflow:" + env.get("PYTHONPATH", "")
            result = subprocess.run(
                ["python", script_path],
                check=True,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            logging.info(result.stdout)
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running {script_path}: {e}")
            logging.error(e.stdout if e.stdout else "No output captured")
            raise
    else:
        logging.error(f"Script not found: {script_path}")
        raise FileNotFoundError(f"Script not found: {script_path}")

def create_task_from_sql(sql_file_path):
    # Creates a task that runs the SQL file using the shared execute_sql_file.py script
    task_id = os.path.splitext(os.path.basename(sql_file_path))[0]
    wrapper_path = "scripts/execute_sql_file.py"
    
    return PythonOperator(
        task_id=task_id,
        python_callable=lambda: run_wrapper_safe(wrapper_path, sql_file_path),
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

def create_task_direct(script_path):
    # Creates a task for a pure Python script
    task_id = os.path.splitext(os.path.basename(script_path))[0]
    return PythonOperator(
        task_id=task_id,
        python_callable=lambda: run_script_direct(script_path),
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

with DAG(
    dag_id="etl_pipeline_safe",
    start_date=datetime(2025, 11, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["ETL", "safe", "modular_sql"]
) as dag:

    # Stage 1: parallel
    db_conn = create_task_direct("scripts/database_connection.py")
    file_load = create_task_direct("scripts/file_loader.py")

    # Stage 2
    file_discover = create_task_direct("scripts/file_discovery.py")
    universal_ingest = create_task_direct("scripts/universal_ingest.py")

    # Stage 1 → Stage 2
    db_conn >> file_discover
    file_load >> file_discover

    # Stage 3: ingestion scripts (these are Python)
    ingestion_folder = "scripts/ingestion"
    ingestion_tasks = []
    if os.path.exists(ingestion_folder):
        for f in sorted(os.listdir(ingestion_folder)):
            if f.endswith(".py") and f.startswith("ingest_"):
                script_path = os.path.join(ingestion_folder, f)
                ingestion_tasks.append(create_task_direct(script_path))

    # Stage 4: Data Cleaning (Auto-discover SQL files)
    # Changed from scripts/cleaning/*.py to sql/clean/*.sql matching colleague's request
    cleaning_folder = "sql/clean"
    lookup_tasks = []
    other_cleaning_tasks = []
    
    if os.path.exists(cleaning_folder):
        for f in sorted(os.listdir(cleaning_folder)):
            if f.endswith(".sql"):
                file_path = os.path.join(cleaning_folder, f)
                task = create_task_from_sql(file_path)
                
                # Separate lookups from other cleaning scripts
                if "lookup_view" in f:
                    lookup_tasks.append(task)
                else:
                    other_cleaning_tasks.append(task)

    # Stage 5: loading scripts (Auto-discover SQL files)
    # Changed from scripts/loading/*.py to sql/load/*.sql
    loading_folder = "sql/load"
    dimension_tasks = []
    fact_order_line_item_task = None
    fact_order_task = None
    
    if os.path.exists(loading_folder):
        for f in sorted(os.listdir(loading_folder)):
            if f.endswith(".sql"):
                file_path = os.path.join(loading_folder, f)
                task = create_task_from_sql(file_path)
                
                # Separate dimensions from facts
                if "dim_" in f.lower():
                    dimension_tasks.append(task)
                elif "fact_order_line_item" in f.lower():
                    fact_order_line_item_task = task
                elif "fact_order" in f.lower():
                    fact_order_task = task

    # Task Dependencies
    
    # Ingestion Trigger
    if ingestion_tasks:
        for task in ingestion_tasks:
            universal_ingest >> task
    
    # 1. Lookups run first (after ingestion)
    if ingestion_tasks and lookup_tasks:
        for ing_task in ingestion_tasks:
            for lookup_task in lookup_tasks:
                ing_task >> lookup_task
    elif lookup_tasks:
        for lookup_task in lookup_tasks:
            universal_ingest >> lookup_task

    # 2. Other cleaning tasks run AFTER lookups
    if lookup_tasks and other_cleaning_tasks:
        for lookup_task in lookup_tasks:
            for clean_task in other_cleaning_tasks:
                lookup_task >> clean_task
    elif ingestion_tasks and other_cleaning_tasks: # Fallback if no lookups
        for ing_task in ingestion_tasks:
            for clean_task in other_cleaning_tasks:
                ing_task >> clean_task
                
    # 3. Dimensions run AFTER cleaning
    if other_cleaning_tasks and dimension_tasks:
        for clean_task in other_cleaning_tasks:
            for dim_task in dimension_tasks:
                clean_task >> dim_task
    
    # 4. FactOrderLineItem runs AFTER Dimensions
    if dimension_tasks and fact_order_line_item_task:
        for dim_task in dimension_tasks:
            dim_task >> fact_order_line_item_task
            
    # 5. FactOrder runs AFTER FactOrderLineItem
    if fact_order_line_item_task and fact_order_task:
        fact_order_line_item_task >> fact_order_task

