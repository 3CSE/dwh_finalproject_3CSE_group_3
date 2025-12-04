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
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, name, credit_card_number, issuing_bank, source_filename
            ORDER BY ingestion_date DESC
        ) AS rn
    FROM cleaned
)
SELECT
    user_id,
    name,
    credit_card_number,
    issuing_bank,
    source_filename,
    ingestion_date
FROM ranked
WHERE rn = 1;

-- View to check cleaned data
SELECT * FROM staging.clean_stg_user_credit_card LIMIT 10;
SELECT * FROM staging.stg_user_credit_card LIMIT 10;



