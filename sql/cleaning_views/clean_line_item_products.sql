-- View: Clean Line Items (Products)
-- Source Table: staging.stg_line_items_products
-- Target Usage: Provides IDs for FactOrderLineItem creation

CREATE OR REPLACE VIEW staging.view_clean_line_items_products AS
SELECT
    -- 1. Identity Columns
    TRIM(order_id) AS order_id,
    TRIM(UPPER(product_id)) AS product_id,

    -- 2. Descriptive Fields: Apply title case
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        INITCAP(TRIM(product_name)), 
                        '''T',  -- Fixes 'T
                        '''t'
                    ),
                    '''S',  -- Fixes 'S
                    '''s'
                ),
                '''D',  -- Fixes 'D
                '''d'
            ),
            '''M',  -- Fixes 'M
            '''m'
        ),
        '''Ll', -- Fixes 'LL
        '''ll'
    ) AS product_name,

    -- 3. Metadata
    source_filename,
    ingestion_date

FROM 
    staging.stg_line_items_products;