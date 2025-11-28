WITH 
-- Get raw product data from staging layer
source_data AS (
    SELECT
        product_id,
        product_name,
        product_type,
        price,
        ingestion_date
    FROM staging.stg_product_list
),
-- Clean and standardize data (e.g., trim spaces, handle NULLs, standardize formats)
cleaned AS (
    SELECT
        TRIM(product_id) AS product_id,
        COALESCE(TRIM(product_name), 'Unknown') AS product_name,
        COALESCE(INITCAP(TRIM(product_type)), 'Unknown') AS product_type,
        CAST(price AS NUMERIC(18,2)) AS price,
    FROM source_data
    WHERE product_id IS NOT NULL
      AND TRIM(product_id) != ''  -- Ensure product_id is not empty after trimming
      AND price IS NOT NULL
      AND price >= 0  -- No negative prices
),

-- Deduplicate EXACT duplicate rows only
-- IMPORTANT: This preserves products with same product_id but different prices/names/types
-- Example: If product_id='P001' has price $10 AND price $20, BOTH are kept
-- Only removes rows where ALL columns (product_id, name, type, price) are identical
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, product_name, product_type, price 
            ORDER BY ingestion_date DESC  -- keep latest per exact duplicate
        ) AS rn
    FROM cleaned
),

-- Keep only the first row per exact duplicate
-- All unique combinations are preserved (including same product_id with different prices)
deduped AS (
    SELECT
        product_id,
        product_name,
        product_type,
        price
    FROM ranked
    WHERE rn = 1
)

-- UPSERT: Insert new records, skip exact duplicates
-- Uses composite unique index on (product_id, product_name, product_type, price)
-- Safe to run multiple times - won't create duplicates or break foreign keys
INSERT INTO warehouse.DimProduct (
    product_id, 
    product_name, 
    product_type, 
    price
)
SELECT 
    product_id, 
    product_name, 
    product_type, 
    price
FROM deduped
ON CONFLICT (product_id, product_name, product_type, price)
DO NOTHING;  -- If exact combination already exists, skip (no update needed)

-- Optional: Verify loaded data
-- SELECT COUNT(*) FROM warehouse.DimProduct;

-- Optional: Data quality check - report products with same ID but different attributes
-- Uncomment to see which product_ids have multiple variations
/*
WITH product_variations AS (
    SELECT 
        product_id,
        COUNT(*) AS record_count,
        COUNT(DISTINCT price) AS price_variations,
        COUNT(DISTINCT product_name) AS name_variations,
        COUNT(DISTINCT product_type) AS type_variations,
        STRING_AGG(DISTINCT price::TEXT, ', ' ORDER BY price::TEXT) AS all_prices
    FROM warehouse.DimProduct
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
SELECT 
    product_id,
    record_count,
    price_variations,
    name_variations,
    type_variations,
    all_prices
FROM product_variations
ORDER BY record_count DESC, product_id;
*/