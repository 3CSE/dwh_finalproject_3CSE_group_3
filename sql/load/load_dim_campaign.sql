-- Load Script: Load DimCampaign
-- Source View: staging.clean_stg_campaign
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

-- check loaded data
-- select count(*) from warehouse.DimCampaign;

-- ensure unknown member exists
INSERT INTO warehouse.DimCampaign (
    campaign_key, campaign_id, campaign_name, campaign_description, discount_value
)
OVERRIDING SYSTEM VALUE
VALUES (
    -1, 'UNKNOWN', 'Unknown Campaign', 'Unknown', 0
)
ON CONFLICT (campaign_key) DO NOTHING;