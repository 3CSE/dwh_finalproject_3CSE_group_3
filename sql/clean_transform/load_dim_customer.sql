-- load_dim_customer.sql
-- Cleans and loads DimCustomer from staging

WITH 
-- Step 1: Get base customer info from staging.stg_user_data
user_data AS (
    SELECT
        user_id,
        TRIM(name) AS name,
        TRIM(gender) AS gender,
        birthdate,
        creation_date,
        INITCAP(TRIM(street)) AS street,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country) AS country,
        TRIM(device_address) AS device_address,
        TRIM(user_type) AS user_type
    FROM staging.stg_user_data
),

-- Step 2: Get job info from staging.stg_user_job
job_data AS (
    SELECT
        user_id,
        TRIM(job_title) AS job_title,
        TRIM(job_level) AS job_level
    FROM staging.stg_user_job
),

-- Step 3: Get credit card info from staging.stg_user_credit_card
credit_data AS (
    SELECT
        user_id,
        TRIM(credit_card_number) AS credit_card_number,
        TRIM(issuing_bank) AS issuing_bank
    FROM staging.stg_user_credit_card
),

-- Step 4: Join all sources together
combined AS (
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
    FROM user_data u
    LEFT JOIN job_data j ON u.user_id = j.user_id
    LEFT JOIN credit_data c ON u.user_id = c.user_id
    WHERE u.user_id IS NOT NULL
),

-- Step 5: Deduplicate by business key (user_id), keep latest ingestion_date if available
deduped AS (
    SELECT *
    FROM combined
    -- You could add ROW_NUMBER() if you have ingestion_date and want latest record
)

-- Step 6: UPSERT into DimCustomer
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
FROM deduped
ON CONFLICT (user_id)
DO NOTHING;  -- Skip if customer_id already exists

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.DimCustomer;
