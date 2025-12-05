-- Load data into DimMerchant table
-- Source: staging.stg_merchant_data
-- Target: warehouse.DimMerchant
-- SCD Type 1 loading script

WITH clean_source AS (
    SELECT
        merchant_id,
        name,
        contact_number,
        street,
        city,
        state,
        country,
        creation_date
    FROM staging.clean_stg_merchant_data
),
ranked_source AS (
    SELECT
        cs.*,
        ROW_NUMBER() OVER (
            PARTITION BY cs.merchant_id
            ORDER BY cs.creation_date DESC
        ) AS row_num
    FROM clean_source cs
)
INSERT INTO warehouse.DimMerchant (
    merchant_id,
    name,
    contact_number,
    street,
    city,
    state,
    country,
    creation_date
)
SELECT
    merchant_id,
    name,
    contact_number,
    street,
    city,
    state,
    country,
    creation_date
FROM ranked_source
WHERE row_num = 1

ON CONFLICT (merchant_id)
DO UPDATE SET
    name = EXCLUDED.name,
    contact_number = EXCLUDED.contact_number,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    country = EXCLUDED.country,
    creation_date = EXCLUDED.creation_date;

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.DimMerchant;

-- Check data loaded
-- SELECT * FROM warehouse.DimMerchant LIMIT 10;

