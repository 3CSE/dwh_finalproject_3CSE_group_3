-- View: Clean Campaign Dimension
-- Source Table: staging.stg_campaign
-- Target Usage: Populates warehouse.DimCampaign

CREATE OR REPLACE VIEW staging.view_clean_campaign AS
SELECT
    -- 1. Identity Columns: Standardize IDs for clean joining
    TRIM(UPPER(campaign_id)) AS campaign_id,

    -- 2. Descriptive Fields: Clean up and format for readability
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        INITCAP(TRIM(campaign_name)), 
                        '''T',  -- Fixes 'T 
                        '''t'
                    ),
                    '''S',  -- Fixes 'S 
                    '''s'
                ),
                '''D',  -- Fixes 'D 
                '''d'
            ),
            '''M',  -- Fixes 'M
            '''m'
        ),
        '''Ll', -- Fixes 'LL
        '''ll'
    ) AS campaign_name,
    
    TRIM(campaign_description) AS campaign_description,

    -- 3. Convert  TEXT 'discount' to clean NUMERIC
    COALESCE(
        -- a. Strip non-numeric/non-decimal characters (removes %, pct, %% etc.)
        CAST(
            NULLIF(
                REGEXP_REPLACE(discount, '[^0-9\.]', '', 'g') 
                , ''
            ) 
        AS NUMERIC(18, 2)), 
        -- b. Handles null
        0.00
    ) AS discount_value

FROM 
    staging.stg_campaign;