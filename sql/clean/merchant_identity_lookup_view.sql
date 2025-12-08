/*
Generates a deterministic Business Key using merchant_id, name, and creation_date.

This business key is propagated and consumed by downstream cleaning views: clean_merchant_data and clean_order_merchant  
*/

CREATE OR REPLACE VIEW staging.merchant_identity_lookup AS
WITH identity_source AS (
    SELECT
        merchant_id,
        name,
        creation_date,
        ingestion_date
    FROM staging.stg_merchant_data
),
cleaned_identity AS (
    SELECT
        TRIM(merchant_id) AS merchant_id,
        COALESCE(NULLIF(NULLIF(INITCAP(TRIM(name)), 'Nan'), ''), 'Unknown') AS name,
        COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '') AS creation_date,
        ingestion_date,

        -- Generate merchant busines key using merchant_id, name, creation_date columns
        MD5(
            LOWER(TRIM(merchant_id)) || '_' ||
            LOWER(TRIM(name)) || '_' ||
            COALESCE(TO_CHAR(creation_date, 'YYYY-MM-DD HH24:MI:SS'), '')
        ) AS merchant_bk
    FROM identity_source
    WHERE merchant_id IS NOT NULL AND TRIM(merchant_id) != ''
),
-- Deduplicate identity rows
dedup_identity AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY merchant_bk
                ORDER BY creation_date ASC, ingestion_date DESC
            ) AS rn
        FROM cleaned_identity
    ) t
    WHERE rn = 1
)
SELECT
    merchant_bk,
    merchant_id,
    name,
    creation_date
FROM dedup_identity;

-- Check created view
-- SELECT * FROM staging.merchant_identity_lookup LIMIT 10;

-- Test if the columns for creating business key is enough (no rows should be returned)
/*
WITH duplicate AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY merchant_id, name, creation_date) AS rn
    FROM staging.clean_stg_merchant_data
) SELECT COUNT(*) FROM duplicate WHERE rn > 1;
*/

