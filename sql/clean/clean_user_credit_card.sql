-- Vince
CREATE OR REPLACE VIEW staging.clean_stg_user_credit_card AS
WITH source_data AS (
    SELECT
        user_id,
        name,
        credit_card_number,
        issuing_bank,
        source_filename,
        ingestion_date
    FROM staging.stg_user_credit_card
),

cleaned AS (
    SELECT
        TRIM(user_id) AS user_id,
        COALESCE(INITCAP(TRIM(name)), 'Unknown') AS name,
        TRIM(credit_card_number) AS credit_card_number,
        COALESCE(LOWER(TRIM(issuing_bank)), 'unknown') AS issuing_bank,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
      AND credit_card_number IS NOT NULL AND TRIM(credit_card_number) != ''
),
-- Remove exact duplicates
dedup_exact AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY user_id, name, credit_card_number, issuing_bank, source_filename
                ORDER BY ingestion_date
            ) AS exact_dup_rank
        FROM cleaned
    ) t
    WHERE exact_dup_rank = 1
),
-- Flag duplicates based on natural key
dup_flag AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY user_id) AS dup_count
    FROM dedup_exact
)
SELECT
    user_id,
    name,
    credit_card_number,
    issuing_bank,
    source_filename,
    ingestion_date,
    (dup_count > 1) AS is_duplicate
FROM dup_flag;

-- Test the view
-- Check count
-- SELECT COUNT(*) FROM staging.clean_stg_user_credit_card;
-- SELECT COUNT(*) FROM staging.stg_user_credit_card;

-- Check the cleaned data
-- SELECT * FROM staging.clean_stg_user_credit_card WHERE is_duplicate=TRUE LIMIT 10;
-- SELECT * FROM staging.stg_user_credit_card LIMIT 10;



