-- Cleans and loads DimCampaign from staging

WITH 
-- Get raw source data from staging
source_data AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        discount,
        ingestion_date
    FROM staging.stg_campaign
),

-- Clean and standardize
-- TRIM text fields, parse discount, filter invalid records
cleaned AS (
    SELECT
        TRIM(campaign_id) AS campaign_id,
        TRIM(campaign_name) AS campaign_name,
        TRIM(campaign_description) AS campaign_description,

        -- Convert discount to numeric fraction
        CASE
            WHEN discount IS NULL THEN NULL
            ELSE (REGEXP_REPLACE(discount, '[^0-9]', '', 'g')::NUMERIC) / 100.0 
        END AS discount_value,
        ingestion_date
    FROM source_data
    WHERE campaign_id IS NOT NULL AND TRIM(campaign_id) != ''
),

-- Deduplicate EXACT duplicates only
-- Preserve variations (e.g., same campaign_id but different discount)
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY campaign_id, campaign_name, campaign_description, discount_value
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
),

deduped AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        discount_value
    FROM ranked
    WHERE rn = 1
)

-- Step 4: Insert into warehouse (UPSERT)
INSERT INTO warehouse.DimCampaign (
    campaign_id,
    campaign_name,
    campaign_description,
    discount_value
)
SELECT
    campaign_id,
    campaign_name,
    campaign_description,
    discount_value
FROM deduped
ON CONFLICT (campaign_id, campaign_name, campaign_description, discount_value)
DO NOTHING;  -- skip exact duplicates

-- Optional: Verify loaded data
-- SELECT * FROM warehouse.DimCampaign;
-- SELECT * FROM staging.stg_campaign;