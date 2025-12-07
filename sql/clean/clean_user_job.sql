-- Vince
CREATE OR REPLACE VIEW staging.clean_stg_user_job AS
WITH source_data AS (
    SELECT
        user_id,
        name,
        job_title,
        job_level,
        source_filename,
        ingestion_date
    FROM staging.stg_user_job
),

cleaned AS (
    SELECT
        TRIM(user_id) AS user_id,
        COALESCE(INITCAP(TRIM(name)), 'Unknown') AS name,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(job_title)), 'Nan'), ''), 'Unknown') AS job_title,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(job_level)), 'Nan'), ''), 'Unknown') AS job_level,
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
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_bk, job_title, job_level
                ORDER BY ingestion_date DESC
            ) AS exact_dup_rank
        FROM keyed_data
    ) t
    WHERE exact_dup_rank = 1
),
-- Flag duplicates based on user_id
dup_count AS (
    SELECT
        *,
        COUNT(user_bk) OVER (PARTITION BY user_id) AS dup_count_value
    FROM dedup_exact
)
SELECT
    user_bk,
    user_id,
    name,
    job_title,
    job_level,
    source_filename,
    ingestion_date,
    (dup_count_value > 1) AS is_duplicate
FROM dup_count;

-- Test the view
-- Check the count
-- SELECT COUNT(*) FROM staging.stg_user_job;
-- SELECT COUNT(*) FROM staging.clean_stg_user_job;

-- Check the cleaned data
-- SELECT * FROM staging.stg_user_job LIMIT 50;
SELECT * FROM staging.clean_stg_user_job LIMIT 50;
/*
with duplicate as (
    select *,
    row_number() over (
        partition by user_bk
        order by ingestion_date desc
    ) as rn
    from staging.clean_stg_user_job
)
select * from duplicate where rn > 1;
*/