-- View: Clean Line Items (Products)
-- Source Table: staging.stg_line_items_products
-- Lookup View: staging.product_identity_lookup (for stable product_bk)
-- Target Usage: Provides IDs for FactOrderLineItem creation

CREATE OR REPLACE VIEW staging.view_clean_line_items_products AS
WITH source_data AS (
    SELECT
        stg.order_id,
        stg.product_name,
        stg.product_id,
        stg.source_filename,
        stg.ingestion_date
    FROM staging.stg_line_items_products stg
),
cleaned AS (
    -- Step 1: Clean and standardize data
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(UPPER(product_id)) AS product_id,

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
    WHERE order_id IS NOT NULL AND TRIM(order_id) != '' 
    AND product_id IS NOT NULL AND TRIM(product_id) != '' 
),
-- Step 2: Join the Identity Lookup to propagate the stable product_bk
with_bk AS (
    SELECT
        c.*,
        lookup.product_bk -- Attach the stable Business Key
    FROM cleaned c
    -- FIX: Changed to LEFT JOIN and simplified the key to the primary source ID.
    -- This ensures all line items are returned, even if the product_bk cannot be found.
    LEFT JOIN staging.product_identity_lookup lookup
        ON c.product_id = lookup.product_id
    -- NOTE: Joining only on product_id relies on the product_bk being unique for that ID, 
    -- which is consistent with Type 2 SCD prep where the lookup handles the definitive identity.
),
ranked AS (
    -- Step 3: Rank duplicates
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id, 
                product_id, 
                product_name, 
                product_bk, -- Include the new BK in the deduplication partition
                source_filename, 
                ingestion_date 
            ORDER BY 
                ingestion_date 
        ) AS row_num,
        
        -- Conflict count based on Line Item (order_id + product_id)
        COUNT(*) OVER (PARTITION BY order_id, product_id) AS conflict_dup_count
    FROM with_bk
)
-- Final SELECT
SELECT
    order_id,
    product_name,
    product_id,
    product_bk, -- Include the stable product BK
    source_filename,
    ingestion_date
 
FROM ranked
WHERE row_num = 1;