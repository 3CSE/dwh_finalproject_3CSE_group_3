-- Vince
CREATE OR REPLACE VIEW staging.clean_stg_orders AS
WITH source_data AS (
    SELECT
        order_id,
        user_id,
        estimated_arrival,
        transaction_date,
        source_filename,
        ingestion_date
    FROM staging.stg_orders
),
-- Clean and standardize
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(user_id) AS user_id,
        CASE
            WHEN estimated_arrival IS NULL OR TRIM(estimated_arrival) = '' THEN NULL
            ELSE REGEXP_REPLACE(estimated_arrival, '[^0-9]', '', 'g')::INT
        END AS estimated_arrival,
        transaction_date,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
      AND user_id IS NOT NULL AND TRIM(user_id) != ''
      AND transaction_date IS NOT NULL
),
-- Deduplicate exact duplicates
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, user_id, estimated_arrival, transaction_date, source_filename
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
)
SELECT
    order_id,
    user_id,
    estimated_arrival,
    transaction_date,
    source_filename,
    ingestion_date
FROM ranked
WHERE rn = 1;


-- Test the view
-- SELECT * FROM staging.stg_orders LIMIT 10;
-- SELECT * FROM staging.clean_stg_orders LIMIT 10;
