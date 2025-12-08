-- View: Clean Line Items (Prices & Quantities)
-- Source Table: staging.stg_line_items_prices
-- Target Usage: Provides pricing and quantity metrics for FactOrderLineItem
-- CRITICAL CHANGE: Removed aggressive deduplication to ensure all line items are preserved 
-- for the positional join. The ROW_NUMBER is now the definitive line item sequence key.

CREATE OR REPLACE VIEW staging.view_clean_line_items_prices AS
WITH source_data AS (
    -- CTE 1: Select the source data
    SELECT
        order_id,
        price,
        quantity,
        source_filename,
        ingestion_date
    FROM staging.stg_line_items_prices
),
cleaned AS (
    -- CTE 2: Apply all core cleaning and standardization
    SELECT
        -- Identity Columns: Clean Order ID for joining
        TRIM(order_id) AS order_id,

        -- Metric: Unit Price (Ensure numeric and non-null)
        CAST(COALESCE(price, 0.00) AS NUMERIC(18, 2)) AS price,

        -- Critical Transformation: Quantity (Strip non-digits and cast to INT)
        CAST(
            NULLIF(REGEXP_REPLACE(quantity, '[^0-9]', '', 'g'), '')
            AS INT
        ) AS quantity_int, -- Rename to avoid confusion

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != '' -- Must have an order ID
),
with_line_total AS (
    -- CTE 3: Calculate the derived metric using cleaned values and apply business rules
    SELECT
        order_id,
        price, -- Original column name
        quantity_int AS quantity,
        (price * quantity_int) AS line_total_amount,
        source_filename,
        ingestion_date
    FROM cleaned
    -- CRITICAL FILTER: Only keep records that have a positive quantity, as they form a valid line item fact.
    WHERE quantity_int IS NOT NULL AND quantity_int > 0 
)
-- Final SELECT: Apply Sequencing ONLY
SELECT
    order_id,
    price,
    quantity,
    line_total_amount,
    source_filename,
    ingestion_date,

    -- CRITICAL: Create the Line Item Sequence Key based on the natural order in the source data.
    -- This sequence is the implicit join key for products.
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY 
            price DESC, 
            quantity DESC,
            ingestion_date, 
            source_filename -- Ensure deterministic ordering
    ) AS line_item_seq
FROM with_line_total
-- NOTE: Removed the is_duplicate flag and the filtering based on ROW_NUMBER.
;