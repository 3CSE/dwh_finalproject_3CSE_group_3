-- View: Clean Line Items (Products)
-- Source Table: staging.stg_line_items_products
-- Target Usage: Provides IDs for FactOrderLineItem creation

CREATE OR REPLACE VIEW staging.view_clean_line_items_products AS
WITH source_data AS (
    SELECT
        order_id,
        product_name,
        product_id,
        source_filename,
        ingestion_date
    FROM staging.stg_line_items_products
),
cleaned AS (
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
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id, 
                product_id, 
                product_name, 
                source_filename, 
                ingestion_date 
            ORDER BY 
                ingestion_date 
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY order_id, product_id) AS conflict_dup_count
    FROM cleaned
)
SELECT
    order_id,
    product_name,
    product_id,
    source_filename,
    ingestion_date,

    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;