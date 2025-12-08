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
)
-- Final SELECT
SELECT
    order_id,
    product_name,
    product_id,
    source_filename,
    ingestion_date,


    ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY 
            product_id, 
            product_name, 
            ingestion_date, 
            source_filename 
    ) AS line_item_seq
FROM cleaned
;