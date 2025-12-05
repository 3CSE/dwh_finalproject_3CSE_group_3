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
        -- Clean job_title
        CASE 
            WHEN job_title ILIKE 'nan' OR job_title IS NULL OR TRIM(job_title) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(job_title))
        END AS job_title,
        -- Clean job_level
        CASE 
            WHEN job_level ILIKE 'nan' OR job_level IS NULL OR TRIM(job_level) = '' 
                THEN 'Unknown'
            ELSE INITCAP(TRIM(job_level))
        END AS job_level,

        source_filename,
        ingestion_date
    FROM source_data
    WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
),
-- Remove exact duplicates
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, name, job_title, job_level, source_filename
                ORDER BY ingestion_date
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
),
-- Flag duplicates based on user_id
dup_flag AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY user_id) AS dup_count
    FROM cleaned_dedup
)
SELECT
    user_id,
    name,
    job_title,
    job_level,
    source_filename,
    ingestion_date,
    (dup_count > 1) AS is_duplicate
FROM dup_flag;

-- Test the view
-- Check the count
-- SELECT COUNT(*) FROM staging.stg_user_job;
-- SELECT COUNT(*) FROM staging.clean_stg_user_job;

-- Check the cleaned data
-- SELECT * FROM staging.stg_user_job LIMIT 50;
-- SELECT * FROM staging.clean_stg_user_job LIMIT 50;