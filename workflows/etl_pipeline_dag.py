from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from datetime import datetime, timedelta
import subprocess
import os
import logging


# -----------------------------
# Helper functions
# -----------------------------


def run_wrapper_safe(wrapper_script, target_file):
    if os.path.exists(wrapper_script) and os.path.exists(target_file):
        try:
            env = os.environ.copy()
            env["PYTHONPATH"] = "/opt/airflow:" + env.get("PYTHONPATH", "")


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
            logging.error(f"Error running {target_file}: {e}")
            logging.error(e.stdout if e.stdout else "No output captured")
            raise
    else:
        raise FileNotFoundError(f"Missing file: {wrapper_script} or {target_file}")




def run_script_direct(script_path):
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
        raise FileNotFoundError(f"Script not found: {script_path}")




def create_task_from_sql(sql_file_path):
    task_id = os.path.splitext(os.path.basename(sql_file_path))[0]
    wrapper_path = "scripts/execute_sql_file.py"


    return PythonOperator(
        task_id=task_id,
        python_callable=lambda: run_wrapper_safe(wrapper_path, sql_file_path),
        retries=2,
        retry_delay=timedelta(minutes=2)
    )




def create_task_direct(script_path):
    task_id = os.path.splitext(os.path.basename(script_path))[0]
    return PythonOperator(
        task_id=task_id,
        python_callable=lambda: run_script_direct(script_path),
        retries=2,
        retry_delay=timedelta(minutes=2)
    )




# -----------------------------
# DAG definition
# -----------------------------


with DAG(
    dag_id="etl_pipeline_safe",
    start_date=datetime(2025, 11, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["ETL", "safe", "modular_sql"]
) as dag:


    # -------------------------
    # Stage 0: INIT (DDL SAFETY)
    # -------------------------
    init_sql_folder = "sql/init"
    init_tasks = []


    if os.path.exists(init_sql_folder):
        for f in sorted(os.listdir(init_sql_folder)):
            if f.endswith(".sql"):
                init_tasks.append(create_task_from_sql(os.path.join(init_sql_folder, f)))


    # -------------------------
    # Stage 1: Bootstrap
    # -------------------------
    db_conn = create_task_direct("scripts/database_connection.py")
    file_load = create_task_direct("scripts/file_loader.py")


    # -------------------------
    # Stage 2: Discovery
    # -------------------------
    file_discover = create_task_direct("scripts/file_discovery.py")
    universal_ingest = create_task_direct("scripts/universal_ingest.py")


    # Init → Stage 1
    for t in init_tasks:
        t >> db_conn
        t >> file_load


    db_conn >> file_discover
    file_load >> file_discover
    file_discover >> universal_ingest


    # -------------------------
    # Stage 3: Ingestion (Python)
    # -------------------------
    ingestion_folder = "scripts/ingestion"
    ingestion_tasks = []


    if os.path.exists(ingestion_folder):
        for f in sorted(os.listdir(ingestion_folder)):
            if f.startswith("ingest_") and f.endswith(".py"):
                ingestion_tasks.append(create_task_direct(os.path.join(ingestion_folder, f)))


    for task in ingestion_tasks:
        universal_ingest >> task


    # -------------------------
    # Stage 4: Cleaning (SQL)
    # -------------------------
    cleaning_folder = "sql/clean"
    lookup_tasks = []
    cleaning_tasks = []


    if os.path.exists(cleaning_folder):
        for f in sorted(os.listdir(cleaning_folder)):
            if f.endswith(".sql"):
                task = create_task_from_sql(os.path.join(cleaning_folder, f))
                if "lookup_view" in f:
                    lookup_tasks.append(task)
                else:
                    cleaning_tasks.append(task)


    for ing in ingestion_tasks:
        for lookup in lookup_tasks:
            ing >> lookup


    for lookup in lookup_tasks:
        for clean in cleaning_tasks:
            lookup >> clean


    # -------------------------
    # Stage 5: Loading (SQL)
    # -------------------------
    loading_folder = "sql/load"
    dimension_tasks = []
    fact_line_item = None
    fact_order = None


    if os.path.exists(loading_folder):
        for f in sorted(os.listdir(loading_folder)):
            if f.endswith(".sql"):
                task = create_task_from_sql(os.path.join(loading_folder, f))
                fname = f.lower()
                if "dim_" in fname:
                    dimension_tasks.append(task)
                elif "fact_order_line_item" in fname:
                    fact_line_item = task
                elif "fact_order" in fname:
                    fact_order = task


    for clean in cleaning_tasks:
        for dim in dimension_tasks:
            clean >> dim


    if fact_line_item:
        for dim in dimension_tasks:
            dim >> fact_line_item


    if fact_line_item and fact_order:
        fact_line_item >> fact_order
