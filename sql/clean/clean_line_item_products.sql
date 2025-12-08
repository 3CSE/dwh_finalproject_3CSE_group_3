-- View: Clean Line Items (Products)
-- Source Table: staging.stg_line_items_products
-- Target Usage: Provides IDs for FactOrderLineItem creation
-- CRITICAL CHANGE: Removed aggressive deduplication to ensure all line items are preserved 
-- for the positional join. The ROW_NUMBER is now the definitive line item sequence key.

CREATE OR REPLACE VIEW staging.view_clean_line_items_products AS
WITH source_data AS (
    -- CTE 1: Select the source data
    SELECT
        order_id,
        product_name,
        product_id,
        source_filename,
        ingestion_date
    FROM staging.stg_line_items_products
),
cleaned AS (
    -- CTE 2: Apply all core cleaning and standardization
    SELECT
        -- Identity Columns: Clean and standardize IDs for joining
        TRIM(order_id) AS order_id,
        TRIM(UPPER(product_id)) AS product_id,

        -- Descriptive Fields: Apply the standardized Title Case logic for consistency
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            INITCAP(TRIM(product_name)), 
                            '''T', '''t'
                        ),
                        '''S', '''s'
                    ),
                    '''D', '''d'
                ),
                '''M', '''m'
            ),
            '''Ll', '''ll'
        ) AS product_name,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != '' -- Must have an order ID
    AND product_id IS NOT NULL AND TRIM(product_id) != '' -- Must have a product ID
)
-- Final SELECT: Apply Sequencing ONLY
SELECT
    order_id,
    product_name,
    product_id,
    source_filename,
    ingestion_date,

    -- CRITICAL: Create the Line Item Sequence Key based on the natural order in the source data.
    -- This sequence is the implicit join key for prices.
    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY 
            product_id, 
            product_name, 
            ingestion_date, 
            source_filename -- Ensure deterministic ordering
    ) AS line_item_seq
FROM cleaned
-- NOTE: Removed is_duplicate flag as it is misleading when line items can share product_id.
;