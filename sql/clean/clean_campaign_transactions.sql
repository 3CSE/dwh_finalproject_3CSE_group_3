-- View: Clean Campaign Transactions
-- Source Table: staging.stg_campaign_transactions
-- Target Usage: Provides Campaign link and dates for FactOrder

CREATE OR REPLACE VIEW staging.view_clean_campaign_transactions AS
WITH source_data AS (
    SELECT
        campaign_id,
        order_id,
        transaction_date,
        estimated_arrival,
        availed,
        source_filename,
        ingestion_date
    FROM staging.stg_campaign_transactions
),
cleaned AS (
    SELECT
        TRIM(UPPER(campaign_id)) AS campaign_id,
        TRIM(order_id) AS order_id,

        transaction_date,
        CAST(NULLIF(REGEXP_REPLACE(estimated_arrival, '[^0-9]', '', 'g'), '') AS INT) AS estimated_arrival,

        CASE 
            WHEN availed = 1 THEN TRUE 
            ELSE FALSE 
        END AS availed,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
    AND campaign_id IS NOT NULL AND TRIM(campaign_id) != ''
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                campaign_id, order_id, transaction_date, estimated_arrival, availed, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY order_id, campaign_id) AS conflict_dup_count
    FROM cleaned
)

SELECT
    campaign_id,
    order_id,
    transaction_date,
    estimated_arrival,
    availed,
    source_filename,
    ingestion_date,

    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;