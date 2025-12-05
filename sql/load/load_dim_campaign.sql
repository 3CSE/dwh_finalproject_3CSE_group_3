-- Load Script: Load DimCampaign
-- Source View: staging.view_clean_campaign
-- Strategy: Type 0/Type 6 SCD (Preserve all unique campaign attribute combinations)

WITH clean_source AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_description,
        discount_value
    FROM staging.view_clean_campaign
)

INSERT INTO warehouse.DimCampaign (
    campaign_id,
    campaign_name,
    campaign_description,
    discount_value
)
SELECT
    cs.campaign_id,
    cs.campaign_name,
    cs.campaign_description,
    cs.discount_value
FROM clean_source cs

WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.DimCampaign dc
    WHERE 
        dc.campaign_id = cs.campaign_id AND
        dc.campaign_name = cs.campaign_name AND
        dc.campaign_description = cs.campaign_description AND
        dc.discount_value = cs.discount_value
);