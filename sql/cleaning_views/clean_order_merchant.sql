-- View: Clean Order Merchant Links
-- Source Table: staging.stg_order_merchant
-- Target Usage: Provides Merchant and Staff keys for FactOrder

CREATE OR REPLACE VIEW staging.view_clean_order_merchant AS
SELECT
    -- 1. Identity Columns
    TRIM(order_id) AS order_id,

    -- merchant_id: Standardize to UPPERCASE 
    TRIM(UPPER(merchant_id)) AS merchant_id,

    -- staff_id: Standardize to UPPERCASE
    TRIM(UPPER(staff_id)) AS staff_id,

    -- 2. Metadata: Pass through for data lineage
    source_filename,
    ingestion_date

FROM 
    staging.stg_order_merchant;