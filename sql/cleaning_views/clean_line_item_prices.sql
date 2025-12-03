-- View: Clean Line Items (Prices & Quantities)
-- Source Table: staging.stg_line_items_prices
-- Target Usage: Provides pricing and quantity metrics for FactOrderLineItem

CREATE OR REPLACE VIEW staging.view_clean_line_items_prices AS
SELECT
    -- 1. Identity Columns
    TRIM(order_id) AS order_id,

    -- 2. Metric: Unit Price
    CAST(COALESCE(price, 0.00) AS NUMERIC(18, 2)) AS price,

    -- 3. Critical Transformation: Quantity
    -- Strip ('PC', 'pcs', 'px', 'pieces') and cast to INT.
    CAST(
        NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '') -- Remove all non-digits
        AS INT
    ) AS quantity,

    -- 4. Derived Metric: Line Total Amount
    CAST(COALESCE(price, 0.00) AS NUMERIC(18, 2)) * CAST(
        NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '')
        AS INT
    ) AS line_total_amount,
    
    -- 5. Metadata
    source_filename,
    ingestion_date

FROM 
    staging.stg_line_items_prices;