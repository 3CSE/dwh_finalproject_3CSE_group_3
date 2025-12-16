-- View: Clean Line Items (Products)
-- Source Table: staging.stg_line_items_products
-- Target Usage: Provides IDs for FactOrderLineItem creation

CREATE OR REPLACE VIEW staging.clean_stg_line_items_products AS
WITH source_data AS (
    SELECT
        order_id, product_name, product_id, source_filename, ingestion_date,
        ctid 
    FROM staging.stg_line_items_products
),
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(UPPER(product_id)) AS product_id,
        TRIM(product_name) AS product_name, 
        source_filename,
        ingestion_date,
        ctid 
    FROM source_data
    WHERE order_id IS NOT NULL
)
SELECT
    order_id, product_name, product_id, source_filename, ingestion_date,
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY ingestion_date, source_filename, ctid
    ) AS line_item_seq
FROM cleaned;