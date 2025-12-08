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
        ) AS quantity_int, 

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != '' 
),
with_line_total AS (

    SELECT
        order_id,
        price, 
        quantity_int AS quantity,
        (price * quantity_int) AS line_total_amount,
        source_filename,
        ingestion_date
    FROM cleaned

    WHERE quantity_int IS NOT NULL AND quantity_int > 0 
)

SELECT
    order_id,
    price,
    quantity,
    line_total_amount,
    source_filename,
    ingestion_date,

    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY 
            price DESC, 
            quantity DESC,
            ingestion_date, 
            source_filename 
    ) AS line_item_seq
FROM with_line_total
;