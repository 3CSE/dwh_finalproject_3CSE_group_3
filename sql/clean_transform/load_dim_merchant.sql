-- load_dim_merchant.sql
-- Cleans and loads DimMerchant from staging

WITH 
-- Step 1: Get base merchant info from staging.stg_merchant_data
merchant_data AS (
    SELECT
        merchant_id,
        TRIM(name) AS name,
        TRIM(contact_number) AS contact_number,
        INITCAP(TRIM(street)) AS street,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country) AS country,
        creation_date
    FROM staging.stg_merchant_data
    WHERE merchant_id IS NOT NULL AND TRIM(merchant_id) != ''
),

-- Step 2: Deduplicate by business key (merchant_id), keep latest ingestion_date if available
deduped AS (
    SELECT *
    FROM merchant_data
    -- Optional: use ROW_NUMBER() over ingestion_date if you have multiple rows per merchant_id
)

-- Step 3: UPSERT into DimMerchant
INSERT INTO warehouse.DimMerchant (
    merchant_id,
    name,
    contact_number,
    street,
    city,
    state,
    country,
    creation_date
)
SELECT
    merchant_id,
    name,
    contact_number,
    street,
    city,
    state,
    country,
    creation_date
FROM deduped
ON CONFLICT (merchant_id)
DO NOTHING;  -- Skip if merchant_id already exists

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.DimMerchant;

