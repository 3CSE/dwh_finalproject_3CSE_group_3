/*
Generates a deterministic Business Key using product_id, product_name, and product_type.

This business key (product_bk) will be consumed by the downstream cleaning view: clean_product_list.
*/

CREATE OR REPLACE VIEW staging.product_identity_lookup AS
WITH identity_source AS (
    SELECT
        product_id,
        product_name,
        product_type,
        ingestion_date
    FROM staging.stg_product_list
),
cleaned_identity AS (
    SELECT
        TRIM(product_id) AS product_id,
        COALESCE(LOWER(TRIM(product_name)), 'unknown') AS product_name,
        COALESCE(LOWER(TRIM(product_type)), 'unknown') AS product_type,
        ingestion_date,

        -- Generate Business Key using the stable product attributes
        MD5(
            LOWER(TRIM(product_id)) || '_' ||
            LOWER(TRIM(product_name)) || '_' ||
            LOWER(TRIM(product_type))
        ) AS product_bk
    FROM identity_source
    WHERE product_id IS NOT NULL AND TRIM(product_id) != ''
),
-- Deduplicate identity rows to ensure one definitive Business Key record exists
dedup_identity AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_bk 
                ORDER BY ingestion_date DESC 
            ) AS rn
        FROM cleaned_identity
    ) t
    WHERE rn = 1
)
SELECT 
    product_id,
    product_bk,
    product_name,
    product_type
FROM dedup_identity;

-- Test if the columns for creating business key is enough (no rows should be returned)
/*
WITH duplicate AS (
    SELECT *,
    -- Columns used in business key generation
    ROW_NUMBER() OVER (PARTITION BY product_id, product_name, product_type) AS rn
    FROM staging.view_clean_product_list
)
SELECT * FROM duplicate WHERE rn > 1;
*/