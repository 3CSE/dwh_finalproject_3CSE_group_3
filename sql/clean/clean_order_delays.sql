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
    WHERE order_id IS NOT NULL 
        AND TRIM(order_id) != ''
        AND delay_in_days IS NOT NULL
),

dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id 
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
)

SELECT
    order_id,
    delay_in_days,
    source_filename,
    ingestion_date
FROM dedup_exact;

-- Test the view
-- Check count
-- SELECT COUNT(*) FROM staging.stg_order_delays;
-- SELECT COUNT(*) FROM staging.clean_stg_order_delays;

-- Check the cleaned data
-- SELECT * FROM staging.clean_stg_order_delays LIMIT 50;
-- SELECT * FROM staging.stg_order_delays LIMIT 50;