-- View: Clean Campaign Dimension
-- Source Table: staging.stg_campaign
-- Target Usage: Populates warehouse.DimCampaign

CREATE OR REPLACE VIEW staging.clean_stg_campaign AS
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
            CAST(NULLIF(REGEXP_REPLACE(discount, '[^0-9\.]', '', 'g'), '') AS NUMERIC(18, 4)) * 0.01,
            0.00
        ) AS discount_value,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE campaign_id IS NOT NULL AND TRIM(campaign_id) != '' 
),
dedup_exact AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY campaign_id 
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
)
SELECT
    campaign_id,
    campaign_name,
    campaign_description,
    discount_value,
    source_filename,
    ingestion_date
FROM dedup_exact;

-- Check cleaned view
-- SELECT * FROM staging.stg_campaign;
-- SELECT * FROM staging.view_clean_campaign;