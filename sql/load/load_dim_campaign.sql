-- Load Script: Load DimCampaign
-- Source View: staging.view_clean_campaign
-- SCD Type 1 loading script for DimCampaign

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
FROM staging.clean_stg_campaign

ON CONFLICT (campaign_id) 
DO UPDATE SET
    -- Update attributes if the ID already exists
    campaign_name = EXCLUDED.campaign_name,
    campaign_description = EXCLUDED.campaign_description,
    discount_value = EXCLUDED.discount_value;