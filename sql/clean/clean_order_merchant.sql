-- View: Clean Order Merchant Links
-- Source Table: staging.stg_order_merchant
-- Target Usage: Provides Merchant and Staff keys for FactOrder

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
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                order_id, merchant_id, staff_id, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY order_id) AS conflict_dup_count
    FROM cleaned
)
SELECT
    order_id,
    merchant_id,
    staff_id,
    source_filename,
    ingestion_date,

    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
WHERE row_num = 1;