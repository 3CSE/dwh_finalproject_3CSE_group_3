-- Load Script: Load DimStaff
-- Source View: staging.view_clean_staff
-- Strategy: Type 1 Slowly Changing Dimension (Overwrite existing attributes)


WITH raw_data AS (
    SELECT
        staff_id,
        name,
        job_level,
        street,
        city,
        state,
        country,
        contact_number,
        creation_date,
        -- Retain ingestion_date here for ranking purposes
        ingestion_date 
    FROM staging.view_clean_staff
),
ranked_source AS (
    SELECT
        staff_id,
        name,
        job_level,
        street,
        city,
        state,
        country,
        contact_number,
        creation_date,
        ingestion_date,
        ROW_NUMBER() OVER (
            PARTITION BY staff_id
            ORDER BY ingestion_date DESC 
        ) AS row_num
    FROM raw_data
)
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
FROM ranked_source

WHERE row_num = 1 

ON CONFLICT (staff_id)
DO UPDATE SET

    name = EXCLUDED.name,
    job_level = EXCLUDED.job_level,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    country = EXCLUDED.country,
    contact_number = EXCLUDED.contact_number


;