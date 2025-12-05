-- Vince
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
        CASE 
            WHEN name IS NULL OR TRIM(name) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(name))
        END AS name,

        -- Address fields (Initcap + Unknown fallback)
        CASE 
            WHEN street IS NULL OR TRIM(street) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(street))
        END AS street,

        CASE 
            WHEN state IS NULL OR TRIM(state) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(state))
        END AS state,

        CASE 
            WHEN city IS NULL OR TRIM(city) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(city))
        END AS city,

        -- Country: do NOT INITCAP long country names
        CASE 
            WHEN country IS NULL OR TRIM(country) = '' 
                THEN 'Unknown'
            ELSE TRIM(country)
        END AS country,

        -- Contact Number Cleaning:
        -- 1. Remove all non-numeric characters
        -- 2. Insert dashes every 3-3-4 (if possible)
        -- 3. Remove accidental leading dashes
        CASE 
            WHEN contact_number IS NULL OR TRIM(contact_number) = '' 
                THEN 'Unknown'
            ELSE (
                SELECT 
                    TRIM(BOTH '-' FROM 
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(contact_number), '[^0-9]', '', 'g'),
                            '(\d{3})(\d{3})(\d+)', 
                            '\1-\2-\3'
                        )
                    )
            )
        END AS contact_number,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE merchant_id IS NOT NULL 
      AND TRIM(merchant_id) != ''
),

-- Remove exact duplicates)
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY merchant_id, creation_date, name, street, state, city, country, contact_number, source_filename
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
),

-- Flag duplicates based on natural key (merchant_id)
dup_flag AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY merchant_id) AS dup_count
    FROM dedup_exact
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
    source_filename,
    ingestion_date,

    CASE 
        WHEN dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate

FROM dup_flag;

-- Test view
-- Check counts
-- SELECT COUNT(*) FROM staging.stg_merchant_data;
-- SELECT COUNT(*) FROM staging.clean_stg_merchant_data;

-- Check the cleaned data
-- SELECT * FROM staging.clean_stg_merchant_data LIMIT 50;
-- SELECT * FROM staging.stg_merchant_data LIMIT 50;
