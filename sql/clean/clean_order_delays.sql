-- Vince
CREATE OR REPLACE VIEW staging.clean_stg_order_delays AS
WITH source_data AS (
    SELECT
        order_id,
        delay_in_days,
        source_filename,
        ingestion_date
    FROM staging.stg_order_delays
),
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        delay_in_days,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
      AND delay_in_days IS NOT NULL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, delay_in_days, source_filename
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
)
SELECT
    order_id,
    delay_in_days,
    source_filename,
    ingestion_date
FROM ranked
WHERE rn = 1;

-- Test the view
-- SELECT * FROM staging.stg_order_delays LIMIT 10;
-- SELECT * FROM staging.clean_stg_order_delays LIMIT 10;