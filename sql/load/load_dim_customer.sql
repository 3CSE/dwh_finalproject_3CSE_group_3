-- Load DimCustomer table
-- Source Views: staging.clean_stg_user_data, staging.clean_stg_user_job, staging.clean_stg_user_credit_card
-- SCD Type 1 loading script 

WITH clean_source AS (
    SELECT
        u.user_id,
        u.name,
        u.gender,
        u.birthdate,
        u.creation_date,
        u.street,
        u.city,
        u.state,
        u.country,
        u.device_address,
        u.user_type,
        j.job_title,
        j.job_level,
        c.credit_card_number,
        c.issuing_bank
    FROM staging.clean_stg_user_data u
    LEFT JOIN staging.clean_stg_user_job j
        ON u.user_id = j.user_id
    LEFT JOIN staging.clean_stg_user_credit_card c
        ON u.user_id = c.user_id
),
ranked_source AS (
    SELECT
        cs.*,
        ROW_NUMBER() OVER (
            PARTITION BY cs.user_id
            ORDER BY cs.creation_date DESC
        ) AS row_num
    FROM clean_source cs
)
INSERT INTO warehouse.DimCustomer (
    user_id,
    name,
    gender,
    birthdate,
    creation_date,
    street,
    city,
    state,
    country,
    device_address,
    user_type,
    job_title,
    job_level,
    credit_card_number,
    issuing_bank
)
SELECT
    user_id,
    name,
    gender,
    birthdate,
    creation_date,
    street,
    city,
    state,
    country,
    device_address,
    user_type,
    job_title,
    job_level,
    credit_card_number,
    issuing_bank
FROM ranked_source
WHERE row_num = 1

ON CONFLICT (user_id)
DO UPDATE SET
    name = EXCLUDED.name,
    gender = EXCLUDED.gender,
    birthdate = EXCLUDED.birthdate,
    creation_date = EXCLUDED.creation_date,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    country = EXCLUDED.country,
    device_address = EXCLUDED.device_address,
    user_type = EXCLUDED.user_type,
    job_title = EXCLUDED.job_title,
    job_level = EXCLUDED.job_level,
    credit_card_number = EXCLUDED.credit_card_number,
    issuing_bank = EXCLUDED.issuing_bank;


-- Optional: Check how many rows loaded (ensure not loading twice)
-- SELECT COUNT(*) FROM warehouse.DimCustomer;

-- Check data
-- SELECT * FROM warehouse.DimCustomer ORDER BY customer_key LIMIT 10;
