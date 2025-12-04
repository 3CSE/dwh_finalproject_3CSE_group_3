-- View: Clean Campaign Dimension
-- Source Table: staging.stg_campaign
-- Target Usage: Populates warehouse.DimCampaign

CREATE OR REPLACE VIEW staging.view_clean_campaign AS
WITH source_data AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        discount,
        source_filename,
        ingestion_date
    FROM staging.stg_campaign
),
cleaned AS (
    SELECT
        TRIM(UPPER(campaign_id)) AS campaign_id,

        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            INITCAP(TRIM(campaign_name)), 
                            '''T', '''t'
                        ),
                        '''S', '''s'
                    ),
                    '''D', '''d'
                ),
                '''M', '''m'
            ),
            '''Ll', '''ll'
        ) AS campaign_name,
        
        TRIM(campaign_description) AS campaign_description,

        COALESCE(
            CAST(
                NULLIF(
                    REGEXP_REPLACE(discount, '[^0-9\.]', '', 'g')
                    , ''
                ) 
            AS NUMERIC(18, 2)), 
            0.00
        ) AS discount_value,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE campaign_id IS NOT NULL AND TRIM(campaign_id) != '' 
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                campaign_id, campaign_name, campaign_description, discount_value, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY campaign_id) AS conflict_dup_count
    FROM cleaned
)
SELECT
    campaign_id,
    campaign_name,
    campaign_description,
    discount_value,
    source_filename,
    ingestion_date,

    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;