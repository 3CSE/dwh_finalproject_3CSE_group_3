from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG
from airflow.utils.helpers import chain
from datetime import datetime, timedelta
import subprocess
import os
import logging

def run_script_safe(script_path):
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
    else:
        logging.error(f"Script not found: {script_path}")

def create_task_safe(script_path):
    task_id = os.path.splitext(os.path.basename(script_path))[0]
    return PythonOperator(
        task_id=task_id,
        python_callable=lambda s=script_path: run_script_safe(s),
        retries=2,
        retry_delay=timedelta(minutes=2)
    )

with DAG(
    dag_id="etl_pipeline_safe",
    start_date=datetime(2025, 11, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["ETL", "safe"]
) as dag:

    # Stage 1: 
    db_conn = create_task_safe("scripts/database_connection.py")
    file_load = create_task_safe("scripts/file_loader.py")

    # Stage 2
    file_discover = create_task_safe("scripts/file_discovery.py")
    universal_ingest = create_task_safe("scripts/universal_ingest.py")

    # Stage 1 → Stage 2
    db_conn >> file_discover
    file_load >> file_discover

    # Stage 3: ingestion scripts
    ingestion_folder = "scripts/ingestion"
    ingestion_tasks = []
    if os.path.exists(ingestion_folder):
        for f in sorted(os.listdir(ingestion_folder)):
            if f.endswith(".py") and f.startswith("ingest_"):
                script_path = os.path.join(ingestion_folder, f)
                ingestion_tasks.append(create_task_safe(script_path))

    # Stage 2 → Stage 3
    if ingestion_tasks:
        for task in ingestion_tasks:
            universal_ingest >> task
