-- Load data into DimMerchant table
-- Source: staging.stg_merchant_data
-- Target: warehouse.DimMerchant
-- SCD Type 2 loading script

WITH 
-- prepare source data
source_data AS (
    SELECT
        merchant_bk,
        merchant_id,
        creation_date,
        name,
        street,
        city,
        state,
        country,
        contact_number
    FROM staging.clean_stg_merchant_data
),

-- detect changes
changes AS (
    SELECT
        s.*,
        d.merchant_key AS dim_key,
        d.merchant_bk AS existing_bk,
        -- check only MUTABLE attributes (Address & Contact)
        -- name and Creation Date are part of the business key generation, so they can't "change" for this key.
        (
            s.street IS DISTINCT FROM d.street OR
            s.city IS DISTINCT FROM d.city OR
            s.state IS DISTINCT FROM d.state OR
            s.country IS DISTINCT FROM d.country OR
            s.contact_number IS DISTINCT FROM d.contact_number
        ) AS is_data_changed
    FROM source_data s
    LEFT JOIN warehouse.DimMerchant d 
        ON s.merchant_bk = d.merchant_bk 
        AND d.is_current = TRUE
),

-- expire old records for changed data
deactivate_old AS (
    UPDATE warehouse.DimMerchant d
    SET 
        is_current = FALSE,
        end_date = CURRENT_TIMESTAMP - INTERVAL '1 second'
    FROM changes c
    WHERE d.merchant_key = c.dim_key
      AND c.is_data_changed = TRUE
    RETURNING d.merchant_key
)

INSERT INTO warehouse.DimMerchant (
    merchant_bk, merchant_id, is_current, effective_date, end_date,
    name, creation_date, street, city, state, country, contact_number
)
SELECT
    merchant_bk, merchant_id, TRUE, 
    CASE 
        WHEN existing_bk IS NULL THEN '1900-01-01'::TIMESTAMP 
        ELSE CURRENT_TIMESTAMP 
    END AS effective_date, 
    NULL,
    name, creation_date, street, city, state, country, contact_number
FROM changes
WHERE
    existing_bk IS NULL     
    OR
    is_data_changed = TRUE;

-- Optional: Check how many rows loadedE
-- SELECT COUNT(*) FROM warehouse.DimMerchant;

-- Check data loaded
-- SELECT * FROM warehouse.DimMerchant LIMIT 10;

-- Ensure Unknown member exists
INSERT INTO warehouse.DimMerchant (
    merchant_key, merchant_bk, merchant_id, name, 
    is_current, effective_date, end_date
)
OVERRIDING SYSTEM VALUE
VALUES (
    -1, 'UNKNOWN', 'UNKNOWN', 'Unknown Merchant', TRUE, '1900-01-01', NULL
)
ON CONFLICT (merchant_key) DO NOTHING;