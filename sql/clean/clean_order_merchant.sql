-- View: Clean Order Merchant Links
-- Source Table: staging.stg_order_merchant
-- Lookup Views: staging.merchant_identity_lookup, staging.staff_identity_lookup
-- Target Usage: Provides Merchant and Staff Business Keys for FactOrder

CREATE OR REPLACE VIEW staging.view_clean_order_merchant AS
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
        TRIM(UPPER(merchant_id)) AS merchant_id,
        TRIM(UPPER(staff_id)) AS staff_id,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
),
with_merchant_bk AS (
    SELECT
        c.*,
        lookup.merchant_bk
    FROM cleaned c

    LEFT JOIN staging.merchant_identity_lookup lookup
        ON c.merchant_id = lookup.merchant_id
),
with_staff_bk AS (
    SELECT
        wmb.*,
        lookup.staff_bk
    FROM with_merchant_bk wmb
    LEFT JOIN staging.staff_identity_lookup lookup
        ON wmb.staff_id = lookup.staff_id
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id, merchant_id, staff_id, merchant_bk, staff_bk, source_filename, ingestion_date
            ORDER BY 
                ingestion_date DESC
        ) AS row_num
    FROM with_staff_bk
)
SELECT
    order_id,
    merchant_id,
    merchant_bk,
    staff_id,
    staff_bk,
    source_filename,
    ingestion_date
 
FROM ranked
WHERE row_num = 1;