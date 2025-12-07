WITH 
-- Get source data from cleaned views
latest_job AS (
    SELECT DISTINCT ON (user_bk) user_bk, job_title, job_level
    FROM staging.clean_stg_user_job
    ORDER BY user_bk, ingestion_date DESC
),
latest_card AS (
    SELECT DISTINCT ON (user_bk) user_bk, credit_card_number, issuing_bank
    FROM staging.clean_stg_user_credit_card
    ORDER BY user_bk, ingestion_date DESC
),
source_stage AS (
    SELECT
        u.user_bk, u.user_id, u.name, u.gender, u.birthdate, u.creation_date,
        u.street, u.city, u.state, u.country, u.device_address, u.user_type,
        COALESCE(j.job_title, 'Unknown') AS job_title,
        COALESCE(j.job_level, 'Unknown') AS job_level,
        COALESCE(c.credit_card_number, 'Unknown') AS credit_card_number,
        COALESCE(c.issuing_bank, 'Unknown') AS issuing_bank
    FROM staging.clean_stg_user_data u
    LEFT JOIN latest_job j ON u.user_bk = j.user_bk
    LEFT JOIN latest_card c ON u.user_bk = c.user_bk
),

-- 2. DETECT CHANGES
changes AS (
    SELECT
        s.*,
        d.customer_key AS dim_key,
        (
            s.name IS DISTINCT FROM d.name OR
            s.street IS DISTINCT FROM d.street OR
            s.city IS DISTINCT FROM d.city OR
            s.state IS DISTINCT FROM d.state OR
            s.country IS DISTINCT FROM d.country OR
            s.device_address IS DISTINCT FROM d.device_address OR
            s.user_type IS DISTINCT FROM d.user_type OR
            s.job_title IS DISTINCT FROM d.job_title OR
            s.job_level IS DISTINCT FROM d.job_level OR
            s.credit_card_number IS DISTINCT FROM d.credit_card_number OR
            s.issuing_bank IS DISTINCT FROM d.issuing_bank
        ) AS is_data_changed,
        d.user_bk AS existing_bk
    FROM source_stage s
    LEFT JOIN warehouse.DimCustomer d 
        ON s.user_bk = d.user_bk 
        AND d.is_current = TRUE
),

-- Expire old records for changed data
deactivate_old AS (
    UPDATE warehouse.DimCustomer d
    SET 
        is_current = FALSE,
        end_date = CURRENT_TIMESTAMP - INTERVAL '1 second'
    FROM changes c
    WHERE d.customer_key = c.dim_key
      AND c.is_data_changed = TRUE
    RETURNING d.customer_key -- Optional: returns IDs that were updated
)

-- Load records into DimCustomer
INSERT INTO warehouse.DimCustomer (
    user_bk, user_id, is_current, effective_date, end_date,
    name, gender, birthdate, creation_date, street, city, state, country,
    device_address, user_type, job_title, job_level, credit_card_number, issuing_bank
)
SELECT
    user_bk, user_id, TRUE, CURRENT_TIMESTAMP, NULL,
    name, gender, birthdate, creation_date, street, city, state, country, 
    device_address, user_type, job_title, job_level, credit_card_number, issuing_bank
FROM changes
WHERE
    existing_bk IS NULL    -- New Users
    OR
    is_data_changed = TRUE; -- Changed Users

-- Test the DimCustomer table
-- select count(*) from warehouse.DimCustomer;
-- select * from warehouse.DimCustomer limit 10;