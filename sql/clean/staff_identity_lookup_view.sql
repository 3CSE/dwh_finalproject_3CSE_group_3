CREATE OR REPLACE VIEW staging.staff_identity_lookup AS
WITH identity_source AS (
    SELECT
        staff_id,
        creation_date,
        ingestion_date
    FROM staging.stg_staff
),
cleaned_identity AS (
    SELECT
        TRIM(staff_id) AS staff_id,
        COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '') AS creation_date,
        ingestion_date,

        -- generate staff business key using staff_id, name, and creation_date columns
        MD5(
            LOWER(TRIM(staff_id)) || '_' ||
            COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '')
        ) AS staff_bk
    FROM identity_source
    WHERE staff_id IS NOT NULL AND TRIM(staff_id) != ''
),
-- deduplicate identity rows
dedup_identity AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY staff_bk
                ORDER BY ingestion_date DESC
            ) AS rn
        FROM cleaned_identity
    ) t
    WHERE rn = 1
)
SELECT
    staff_id,
    staff_bk,
    creation_date
FROM dedup_identity;

-- Test if the columns for creating business key is enough (no rows should be returned)
/*
WITH duplicate AS (
    SELECT *,
    -- Columns used in business key generation
    ROW_NUMBER() OVER (PARTITION BY staff_id, creation_date) AS rn
    FROM staging.view_clean_staff
)
SELECT * FROM duplicate WHERE rn > 1;
*/
