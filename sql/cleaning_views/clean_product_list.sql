-- View: Clean Product Dimension
-- Source Table: staging.stg_product_list
-- Target Usage: Populates warehouse.DimProduct and FactOrderLineItem

CREATE OR REPLACE VIEW staging.view_clean_product_list AS
SELECT
    -- 1. Identity Columns: Clean and standardize ID for joining
    TRIM(UPPER(product_id)) AS product_id,

    -- 2. Descriptive Fields: Title Case 
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

    -- 3. Category Field: Convert snake_case or messy casing to Title Case for readability
    INITCAP(REPLACE(TRIM(product_type), '_', ' ')) AS product_type,

    -- 4. Metric Field: Ensure price is non-null
    COALESCE(price, 0.00) AS price,
    
    -- 5. Metadata: Pass through for data lineage
    source_filename,
    ingestion_date

FROM 
    staging.stg_product_list;