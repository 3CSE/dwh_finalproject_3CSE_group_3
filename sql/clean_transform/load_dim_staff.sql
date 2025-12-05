-- load_dim_staff.sql
-- Cleans and loads DimStaff from staging

WITH 
-- Step 1: Get base staff info from staging.stg_staff
staff_data AS (
    SELECT
        staff_id,
        TRIM(name) AS name,
        TRIM(job_level) AS job_level,
        INITCAP(TRIM(street)) AS street,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country) AS country,
        TRIM(contact_number) AS contact_number,
        creation_date
    FROM staging.stg_staff
    WHERE staff_id IS NOT NULL AND TRIM(staff_id) != ''
),

-- Step 2: Deduplicate by business key (staff_id)
deduped AS (
    SELECT *
    FROM staff_data
    -- Optional: use ROW_NUMBER() over ingestion_date if multiple rows per staff_id
)

-- Step 3: UPSERT into DimStaff
INSERT INTO warehouse.DimStaff (
    staff_id,
    name,
    job_level,
    street,
    city,
    state,
    country,
    contact_number,
    creation_date
)
SELECT
    staff_id,
    name,
    job_level,
    street,
    city,
    state,
    country,
    contact_number,
    creation_date
FROM deduped
ON CONFLICT (staff_id)
DO NOTHING;  -- Skip if staff_id already exists

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.DimStaff;
