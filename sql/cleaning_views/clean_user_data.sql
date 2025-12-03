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
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, creation_date, name, street, state, city, country,
                         birthdate, gender, device_address, user_type, source_filename
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
)
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
FROM ranked
WHERE rn = 1;

-- test view
-- SELECT * FROM staging.clean_stg_user_data LIMIT 20;
-- SELECT * FROM staging.stg_user_data LIMIT 20;