-- View: Clean Line Items (Prices & Quantities)
-- Source Table: staging.stg_line_items_prices
-- Target Usage: Provides pricing and quantity metrics for FactOrderLineItem

CREATE OR REPLACE VIEW staging.view_clean_line_items_prices AS
WITH source_data AS (
    SELECT
        order_id,
        price,
        quantity,
        source_filename,
        ingestion_date
    FROM staging.stg_line_items_prices
),
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        CAST(COALESCE(price, 0.00) AS NUMERIC(18, 2)) AS price,
        CAST(
            NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '')
            AS INT
        ) AS quantity,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != '' 
),
with_line_total AS (
    SELECT
        order_id,
        price,
        quantity,
        (price * quantity) AS line_total_amount,
        source_filename,
        ingestion_date
    FROM cleaned
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id, price, quantity, line_total_amount, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        COUNT(*) OVER (PARTITION BY order_id) AS conflict_dup_count
    FROM with_line_total
)
SELECT
    order_id,
    price,
    quantity,
    line_total_amount,
    source_filename,
    ingestion_date,
    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;