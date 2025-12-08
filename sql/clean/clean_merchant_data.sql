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
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(name)), 'Nan'), ''), 'Unknown') AS name,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(street)), 'Nan'), ''), 'Unknown') AS street,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(state)), 'Nan'), ''), 'Unknown') AS state,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(city)), 'Nan'), ''), 'Unknown') AS city,
        COALESCE(NULLIF(NULLIF(TRIM(country), 'Nan'), ''), 'Unknown') AS country,

        -- Contact number: Clean and format
        CASE 
            WHEN contact_number IS NULL OR TRIM(contact_number) = '' 
                THEN 'Unknown'
            ELSE TRIM(BOTH '-' FROM 
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(contact_number), '[^0-9]', '', 'g'),
                            '(\d{3})(\d{3})(\d+)', 
                            '\1-\2-\3'))
        END AS contact_number,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE merchant_id IS NOT NULL 
      AND TRIM(merchant_id) != ''
),

keyed_data AS (
    SELECT
        t1.*,
        t2.merchant_bk
    FROM cleaned t1
    JOIN staging.merchant_identity_lookup t2
    ON t1.merchant_id = t2.merchant_id AND t1.name = t2.name
    AND COALESCE(TO_CHAR(t1.creation_date, 'YYYY-MM-DD HH24:MI:SS'), '') = t2.creation_date
),

-- Remove exact duplicates
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY merchant_bk, creation_date, street, state, city, country, contact_number
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM keyed_data
    ) t
    WHERE exact_dup_rank = 1
),

-- count duplicates based on natural key (merchant_id)
dup_count AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY merchant_id) AS dup_count_value
    FROM dedup_exact
)

SELECT
    merchant_bk,
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
    (dup_count_value > 1) AS is_duplicate

FROM dup_count;

-- Test view
-- Check counts
-- SELECT COUNT(*) FROM staging.stg_merchant_data;
-- SELECT COUNT(*) FROM staging.clean_stg_merchant_data;

-- Check the cleaned data
-- SELECT * FROM staging.clean_stg_merchant_data LIMIT 50;
-- SELECT * FROM staging.stg_merchant_data LIMIT 50;
