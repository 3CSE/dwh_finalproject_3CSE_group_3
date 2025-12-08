-- View: Clean Staff Dimension
-- Source Table: staging.stg_staff
-- Target Usage: Populates warehouse.DimStaff

CREATE OR REPLACE VIEW staging.view_clean_staff AS
WITH source_data AS (
    SELECT
        staff_id,
        name,
        job_level,
        street,
        state,
        city,
        country,
        contact_number,
        creation_date,
        source_filename,
        ingestion_date
    FROM staging.stg_staff
),
cleaned AS (
    SELECT
        TRIM(UPPER(staff_id)) AS staff_id,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(name)), 'Nan'), ''), 'Unknown') AS name,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(job_level)), 'Nan'), ''), 'Unknown') AS job_level,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(street)), 'Nan'), ''), 'Unknown') AS street,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(city)), 'Nan'), ''), 'Unknown') AS city,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(state)), 'Nan'), ''), 'Unknown') AS state,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(country)), 'Nan'), ''), 'Unknown') AS country,
        REGEXP_REPLACE(contact_number, '[^0-9]', '', 'g') AS contact_number,
        creation_date,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE staff_id IS NOT NULL AND TRIM(staff_id) != ''
),
keyed_data AS (
    SELECT 
        t1.*,
        t2.staff_bk
    FROM cleaned t1
    JOIN staging.staff_identity_lookup t2
    ON t1.staff_id = t2.staff_id AND COALESCE(TO_CHAR(t1.creation_date, 'YYYY-MM-DD HH24:MI:SS'), '') = t2.creation_date
),
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY staff_bk, creation_date, job_level, street, state, city, country, contact_number
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM keyed_data
    ) t
    WHERE exact_dup_rank = 1
),
dup_count AS (
    SELECT *,
    COUNT(staff_bk) OVER (PARTITION BY staff_id) AS dup_count_value
    FROM dedup_exact
)
SELECT
    staff_bk,
    staff_id,
    name,
    job_level,
    street,
    city,
    state,
    country,
    contact_number,
    creation_date,
    source_filename,
    ingestion_date,
    (dup_count_value > 1) is_duplicate
FROM dup_count;

-- Test view
-- Check counts
-- SELECT COUNT(*) FROM staging.stg_merchant_data;
-- SELECT COUNT(*) FROM staging.view_clean_staff;

-- Check the cleaned data
-- SELECT * FROM staging.view_clean_staff WHERE is_duplicate = TRUE LIMIT 50;
-- SELECT * FROM staging.stg_merchant_data LIMIT 50;