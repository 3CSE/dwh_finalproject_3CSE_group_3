-- View: Clean Order Merchant Links
-- Source Table: staging.stg_order_merchant
-- Lookup Views: staging.merchant_identity_lookup, staging.staff_identity_lookup
-- Target Usage: Provides Merchant and Staff Business Keys for FactOrder

CREATE OR REPLACE VIEW staging.clean_stg_order_merchant AS
WITH source_data AS (
    SELECT
        order_id,
        merchant_id,
        staff_id,
        source_filename,
        ingestion_date
    FROM staging.stg_order_merchant
),
cleaned AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(merchant_id) AS merchant_id,
        TRIM(staff_id) AS staff_id,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
),
keyed_data AS (
    SELECT
        c.order_id, c.merchant_id, c.staff_id,
        m_lookup.merchant_bk, s_lookup.staff_bk,
        c.source_filename, c.ingestion_date
    FROM cleaned c
    LEFT JOIN staging.merchant_identity_lookup m_lookup ON c.merchant_id = m_lookup.merchant_id
    LEFT JOIN staging.staff_identity_lookup s_lookup ON c.staff_id = s_lookup.staff_id
),
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, merchant_bk, staff_bk 
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM keyed_data
    ) t
    WHERE exact_dup_rank = 1
)
SELECT
    order_id,
    merchant_bk,
    staff_bk,
    merchant_id,
    staff_id,
    source_filename,
    ingestion_date
FROM dedup_exact;

-- Check view
SELECT * FROM staging.clean_stg_order_merchant LIMIT 10;
-- SELECT COUNT(*) FROM staging.stg_order_merchant;