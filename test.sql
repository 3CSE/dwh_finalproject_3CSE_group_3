SELECT dag_id, task_id, run_id, start_date, end_date, state
FROM task_instance
WHERE dag_id = 'etl_pipeline_safe'
  AND task_id LIKE 'ingest_%'
ORDER BY start_date DESC;
