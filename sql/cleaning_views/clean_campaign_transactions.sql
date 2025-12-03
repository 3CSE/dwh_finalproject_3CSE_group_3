-- View: Clean Campaign Transactions
-- Source Table: staging.stg_campaign_transactions
-- Target Usage: Populates warehouse.FactOrder

CREATE OR REPLACE VIEW staging.view_clean_campaign_transactions AS
SELECT
    -- 1. Identity Columns: Clean and standardize for joining
    TRIM(UPPER(campaign_id)) AS campaign_id,
    TRIM(order_id) AS order_id,

    -- 2. Date Columns: 
    transaction_date,

    -- 3. Derived Columns: renamed estimated_arrival to estimated_arrival_days for clarity
    CAST(NULLIF(REGEXP_REPLACE(estimated_arrival, '[^0-9]', '', 'g'), '') AS INT) AS estimated_arrival_days,

    -- Convert availed to boolean
    CASE 
        WHEN availed = 1 THEN TRUE 
        ELSE FALSE 
    END AS availed,

    -- 4. Metadata
    source_filename,
    ingestion_date

FROM 
    staging.stg_campaign_transactions;

    