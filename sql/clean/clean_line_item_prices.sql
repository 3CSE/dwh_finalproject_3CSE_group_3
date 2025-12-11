-- View: Clean Line Items (Prices & Quantities)
-- Source Table: staging.stg_line_items_prices
-- Target Usage: Provides pricing and quantity metrics for FactOrderLineItem

CREATE OR REPLACE VIEW staging.view_clean_line_items_prices AS
WITH source_data AS (
    SELECT
        order_id, price, quantity, source_filename, ingestion_date,
        ctid 
    FROM staging.stg_line_items_prices
),
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        CAST(COALESCE(price, 0.00) AS NUMERIC(18, 2)) AS price,
        CAST(NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') AS INT) AS quantity_int, 
        source_filename,
        ingestion_date,
        ctid 
    FROM source_data
    WHERE order_id IS NOT NULL
),
with_line_total AS (
    SELECT
        order_id, price, quantity_int AS quantity, (price * quantity_int) AS line_total_amount,
        source_filename, ingestion_date,
        ctid 
    FROM cleaned
    WHERE quantity_int > 0 
)
SELECT
    order_id, price, quantity, line_total_amount, source_filename, ingestion_date,
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY ingestion_date, source_filename, ctid
    ) AS line_item_seq
FROM with_line_total;