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
        
        CAST(
            CASE 
                WHEN COALESCE(price, 0.00) < 0 THEN 0.00
                ELSE COALESCE(price, 0.00) 
            END 
        AS NUMERIC(18, 2)) AS price,
        
        CAST(
            NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '')
            AS INT
        ) AS quantity,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE 
        order_id IS NOT NULL AND TRIM(order_id) != ''
        AND CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') AS INT) >= 0
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
                ingestion_date DESC 
        ) AS row_num
    FROM with_line_total
)
SELECT
    order_id,
    price,
    quantity,
    line_total_amount,
    source_filename,
    ingestion_date
 
FROM ranked
WHERE row_num = 1;