-- View: Clean Product Dimension
-- Source Table: staging.stg_product_list
-- Target Usage: Populates warehouse.DimProduct and FactOrderLineItem

CREATE OR REPLACE VIEW staging.view_clean_product_list AS
WITH source_data AS (
    SELECT
        product_id,
        product_name,
        product_type,
        price,
        source_filename,
        ingestion_date
    FROM staging.stg_product_list
),
cleaned AS (
    SELECT
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

        INITCAP(REPLACE(TRIM(product_type), '_', ' ')) AS product_type,

        COALESCE(price, 0.00) AS price,
        
        source_filename,
        ingestion_date
    FROM source_data
    WHERE product_id IS NOT NULL AND TRIM(product_id) != '' 
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                product_id, product_name, product_type, price, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY product_id) AS conflict_dup_count
    FROM cleaned
)
SELECT
    product_id,
    product_name,
    product_type,
    price,
    source_filename,
    ingestion_date,

    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;