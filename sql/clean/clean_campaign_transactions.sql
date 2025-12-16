-- View: Clean Campaign Transactions
-- Source Table: staging.stg_campaign_transactions
-- Target Usage: Provides Campaign link and dates for FactOrder

CREATE OR REPLACE VIEW staging.clean_stg_campaign_transactions AS
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
        TRIM(campaign_id) AS campaign_id,
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
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                -- STRICT DEDUPLICATION: Partition by the Composite Key
                PARTITION BY campaign_id, order_id
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
)
SELECT
    campaign_id,
    order_id,
    transaction_date,
    estimated_arrival,
    availed,
    source_filename,
    ingestion_date
FROM dedup_exact;

-- Check cleaned view
-- SELECT count(*) FROM staging.stg_campaign_transactions;
-- SELECT count(*) FROM staging.clean_stg_campaign_transactions;