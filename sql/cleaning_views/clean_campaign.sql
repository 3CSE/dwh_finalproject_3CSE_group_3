-- View: Clean Campaign Dimension
-- Source Table: staging.stg_campaign
-- Target Usage: Populates warehouse.DimCampaign

CREATE OR REPLACE VIEW staging.view_clean_campaign AS
SELECT
    -- 1. Identity Columns: Standardize IDs for clean joining
    TRIM(UPPER(campaign_id)) AS campaign_id,

    -- 2. Descriptive Fields: Clean up and format for readability
    -- FIX: Replaced environment-sensitive REGEXP_REPLACE with safe string manipulation
    -- 1. Apply INITCAP to get base title case (e.g., "Wouldn'T You Know It")
    -- 2. Use REPLACE to fix common contraction issues (e.g., 'T -> 't)
    REPLACE(
        REPLACE(
            REPLACE(
                INITCAP(TRIM(campaign_name)), 
                '''T',  -- Fixes 'T in wouldn'T
                '''t'
            ),
            '''S',  -- Fixes 'S in Shopzada'S
            '''s'
        ),
        '''D',  -- Fixes 'D in she'D
        '''d'
    ) AS campaign_name,
    
    TRIM(campaign_description) AS campaign_description,

    -- 3. Critical Transformation: Convert messy TEXT 'discount' to clean NUMERIC
    COALESCE(
        -- a. Strip non-numeric/non-decimal characters (removes %, pct, %% etc.)
        CAST(
            NULLIF(
                REGEXP_REPLACE(discount, '[^0-9\.]', '', 'g') -- Keep only digits and dots
                , ''
            ) 
        AS NUMERIC(18, 2)), 
        -- b. Handle NULL/empty strings by defaulting to 0.00
        0.00
    ) AS discount_value

FROM 
    staging.stg_campaign;