-- Vince
CREATE OR REPLACE VIEW staging.clean_stg_user_data AS
WITH source_data AS (
    SELECT
        user_id,
        creation_date,
        name,
        street,
        state,
        city,
        country,
        birthdate,
        gender,
        device_address,
        user_type,
        source_filename,
        ingestion_date
    FROM staging.stg_user_data
),
cleaned AS (
    SELECT
        TRIM(user_id) AS user_id,
        creation_date,
        COALESCE(INITCAP(TRIM(name)), 'Unknown') AS name,
        COALESCE(TRIM(street), 'Unknown') AS street,
        COALESCE(INITCAP(TRIM(state)), 'Unknown') AS state,
        COALESCE(INITCAP(TRIM(city)), 'Unknown') AS city,
        COALESCE(INITCAP(TRIM(country)), 'Unknown') AS country,
        birthdate,
        COALESCE(LOWER(TRIM(gender)), 'unknown') AS gender,
        COALESCE(TRIM(device_address), 'unknown') AS device_address,
        COALESCE(LOWER(TRIM(user_type)), 'unknown') AS user_type,
        source_filename,
        ingestion_date,
        -- New Business Key Generation (concatenated user_id, birthdate, gender, and creation_date)
        -- These columns doesn't change or at least often
        MD5(
            LOWER(TRIM(user_id)) || '_' ||
            COALESCE(TO_CHAR(birthdate,'YYYY-MM-DD HH24:MI:SS'),'') || '_' ||
            LOWER(TRIM(gender)) || '_' ||
            COALESCE(TO_CHAR(creation_date,'YYYY-MM-DD HH24:MI:SS'),'')
        ) AS business_key
    FROM source_data
    WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
),
-- Remove exact duplicates
dedup_exact AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY business_key, name, street, state, city, country, device_address, user_type
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
),
-- count duplicates based on natural key
dup_count AS (
    SELECT
        *,
        COUNT(business_key) OVER (PARTITION BY user_id) AS dup_count_value
    FROM dedup_exact
)
SELECT
    business_key,
    user_id,
    creation_date,
    name,
    street,
    state,
    city,
    country,
    birthdate,
    gender,
    device_address,
    user_type,
    source_filename,
    ingestion_date,
    (dup_count_value > 1) AS is_duplicate
FROM dup_count;

-- TEST VIEW
-- check count
-- SELECT COUNT(*) FROM staging.clean_stg_user_data;
-- SELECT COUNT(*) FROM staging.stg_user_data;

-- Check cleaned data
-- SELECT COUNT(*) FROM staging.clean_stg_user_data WHERE is_duplicate = TRUE;
-- SELECT * FROM staging.stg_user_data LIMIT 20;

-- Test if the columns for creating business key is enough
/*
WITH duplicate AS (
    SELECT *,
    -- Columns used in business key generation
    ROW_NUMBER() OVER (PARTITION BY user_id, gender, birthdate) AS rn
    FROM staging.clean_stg_user_data
)
SELECT * FROM duplicate WHERE rn > 1;
*/
