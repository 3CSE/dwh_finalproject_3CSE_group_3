DROP VIEW IF EXISTS staging.user_identity_lookup CASCADE;
CREATE OR REPLACE VIEW staging.user_identity_lookup AS
WITH identity_source AS (
    SELECT
        user_id,
        name,
        birthdate,
        gender,
        creation_date,
        ingestion_date  
    FROM staging.stg_user_data
),
cleaned_identity AS (
    SELECT
        TRIM(user_id) AS user_id,
        COALESCE(INITCAP(TRIM(name)), 'Unknown') AS name,
        COALESCE(TO_CHAR(birthdate, 'YYYY-MM-DD HH24:MI:SS'), '') AS birthdate,
        LOWER(TRIM(gender)) AS gender,
        COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '') AS creation_date,
        ingestion_date,
        
        -- Generate Business Key using user_id, name, birthdate, gender, creation_date columns
        MD5(
            LOWER(TRIM(user_id)) || '_' || 
            LOWER(TRIM(name)) || '_' ||    
            COALESCE(TO_CHAR(birthdate, 'YYYY-MM-DD HH24:MI:SS'), '') || '_' ||
            LOWER(TRIM(gender)) || '_' ||
            COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '')
        ) AS user_bk
    FROM identity_source
    WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
),
-- Deduplicate identity rows to ensure one definitive Business Key record exists
dedup_identity AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY user_bk 
                ORDER BY creation_date ASC, ingestion_date DESC 
            ) AS rn
        FROM cleaned_identity
    ) t
    WHERE rn = 1
)
SELECT 
    user_id,
    user_bk,
    name,
    birthdate,
    gender,
    creation_date
FROM dedup_identity;

-- Test if the columns for creating business key is enough (no rows should be returned)
/*
WITH duplicate AS (
    SELECT *,
    -- Columns used in business key generation
    ROW_NUMBER() OVER (PARTITION BY user_id, name, birthdate, gender, creation_date) AS rn
    FROM staging.clean_stg_user_data
)
SELECT * FROM duplicate WHERE rn > 1;
*/
