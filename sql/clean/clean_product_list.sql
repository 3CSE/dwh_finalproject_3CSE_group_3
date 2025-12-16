-- View: Clean Product List
-- Source Table: staging.stg_product_list

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
        CASE
            WHEN COALESCE(price, 0.00) < 0 THEN 0.00
            ELSE COALESCE(price, 0.00)
        END AS price,
        
        source_filename,
        ingestion_date
    FROM source_data
    WHERE product_id IS NOT NULL 
      AND TRIM(product_id) != '' 
      AND TRIM(UPPER(product_id)) NOT IN ('N/A', 'NA', 'UNKNOWN', 'NULL', 'INVALID', 'NOT APPLICABLE')
),
with_bk_and_hash AS (
    -- CTE 3: Generate the Business Key (product_bk) and the SCD Type 2 Change Detection Hash
    SELECT
        *,
        MD5(product_id) AS product_bk,
        
        MD5(
            COALESCE(product_name, '') ||
            COALESCE(product_type, '') ||
            COALESCE(CAST(price AS TEXT), '')
        ) AS product_attribute_hash
    FROM cleaned
),
ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY 
                ingestion_date DESC,
                source_filename DESC
        ) AS row_num
        
    FROM with_bk_and_hash
)

SELECT
    product_id,
    product_bk,
    product_name,
    product_type,
    price,
    product_attribute_hash,
    source_filename,
    ingestion_date
 
FROM ranked
WHERE row_num = 1;