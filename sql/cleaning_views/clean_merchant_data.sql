-- Vince
-- NOTE: Standardization of phone number column is still questionable 
CREATE OR REPLACE VIEW staging.clean_stg_merchant_data AS
WITH source_data AS (
    SELECT
        merchant_id,
        creation_date,
        name,
        street,
        state,
        city,
        country,
        contact_number,
        source_filename,
        ingestion_date
    FROM staging.stg_merchant_data
),
cleaned AS (
    SELECT
        TRIM(merchant_id) AS merchant_id,
        creation_date,

        -- Merchant name
        COALESCE(INITCAP(TRIM(name)), 'Unknown') AS name,

        -- Addresses (simple initcap cleaning)
        COALESCE(INITCAP(TRIM(street)), 'Unknown') AS street,
        COALESCE(INITCAP(TRIM(state)), 'Unknown') AS state,
        COALESCE(INITCAP(TRIM(city)), 'Unknown') AS city,

        -- Country: preserve proper format for long names
        CASE 
            WHEN country IS NULL OR TRIM(country) = '' 
                THEN 'Unknown'
            ELSE TRIM(country)
        END AS country,

        -- Contact number: normalize
        CASE 
            WHEN contact_number IS NULL OR TRIM(contact_number) = '' 
                THEN 'Unknown'
            ELSE REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(contact_number), '[^0-9]+', '-', 'g'),
                '^-',  
                ''
            )
        END AS contact_number,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE merchant_id IS NOT NULL AND TRIM(merchant_id) != ''
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY merchant_id
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
)

SELECT
    merchant_id,
    creation_date,
    name,
    street,
    state,
    city,
    country,
    contact_number,
    source_filename
FROM ranked
WHERE rn = 1;

-- Test view
-- SELECT * FROM staging.clean_stg_merchant_data LIMIT 50;
-- SELECT * FROM staging.stg_merchant_data LIMIT 50;
