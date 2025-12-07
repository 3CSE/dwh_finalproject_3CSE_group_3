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
        ingestion_date
    FROM source_data
    WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
),
keyed_data AS (
    SELECT
        t1.*,
        t2.user_bk
    FROM cleaned t1
    JOIN staging.user_identity_lookup t2
    ON t1.user_id = t2.user_id AND t1.name = t2.name
),
-- Remove exact duplicates
dedup_exact AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY user_bk, creation_date, birthdate, gender, street, state, city, country, device_address, user_type
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM keyed_data
    ) t
    WHERE exact_dup_rank = 1
),
-- count duplicates based on natural key
dup_count AS (
    SELECT
        *,
        COUNT(user_bk) OVER (PARTITION BY user_id) AS dup_count_value
    FROM dedup_exact
)
SELECT
    user_bk,
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
-- SELECT * FROM staging.clean_stg_user_data LIMIT 20;
-- SELECT * FROM staging.stg_user_data LIMIT 20;