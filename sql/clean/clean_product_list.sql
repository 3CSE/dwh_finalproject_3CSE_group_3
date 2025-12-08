-- View: Clean Product Dimension
-- Source Table: staging.stg_product_list
-- Lookup View: staging.product_identity_lookup

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
    WHERE product_id IS NOT NULL AND TRIM(product_id) != '' 
),
with_bk AS (
    SELECT
        c.*,
        lookup.product_bk -- Attach the stable Business Key
    FROM cleaned c
    INNER JOIN staging.product_identity_lookup lookup
        ON c.product_id = lookup.product_id
        AND LOWER(c.product_name) = lookup.product_name 
        AND LOWER(c.product_type) = lookup.product_type 
),

hashed AS (
    SELECT
        *,

        MD5(
            COALESCE(CAST(price AS TEXT), '')
        ) AS product_attribute_hash
    FROM with_bk
),
ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                product_id, product_name, product_type, price, product_bk, product_attribute_hash, source_filename, ingestion_date
            ORDER BY 
                ingestion_date DESC 
        ) AS row_num
        

    FROM hashed
)
-- Final SELECT: Filter to the latest exact match and include the new keys
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