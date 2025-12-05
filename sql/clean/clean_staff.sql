-- View: Clean Staff Dimension
-- Source Table: staging.stg_staff
-- Target Usage: Populates warehouse.DimStaff

CREATE OR REPLACE VIEW staging.view_clean_staff AS
WITH source_data AS (
    SELECT
        staff_id,
        name,
        job_level,
        street,
        state,
        city,
        country,
        contact_number,
        creation_date,
        source_filename,
        ingestion_date
    FROM staging.stg_staff
),
cleaned AS (
    SELECT
        TRIM(UPPER(staff_id)) AS staff_id,

        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            INITCAP(TRIM(name)), 
                            '''T', '''t'
                        ),
                        '''S', '''s'
                    ),
                    '''D', '''d'
                ),
                '''M', '''m'
            ),
            '''Ll', '''ll'
        ) AS name,

        INITCAP(TRIM(job_level)) AS job_level,
        INITCAP(TRIM(street)) AS street,
        INITCAP(TRIM(city)) AS city,
        INITCAP(TRIM(state)) AS state,
        INITCAP(TRIM(country)) AS country,

        REGEXP_REPLACE(contact_number, '[^0-9]', '', 'g') AS contact_number,
        creation_date,
        source_filename,
        ingestion_date
    FROM source_data
    WHERE staff_id IS NOT NULL AND TRIM(staff_id) != ''
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                staff_id, name, job_level, street, city, state, country, contact_number, creation_date, source_filename, ingestion_date
            ORDER BY 
                ingestion_date
        ) AS row_num,
        
        COUNT(*) OVER (PARTITION BY staff_id) AS conflict_dup_count
    FROM cleaned
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
    creation_date,
    source_filename,
    ingestion_date,

    -- Flag is TRUE if the primary key appears more than once (conflict)
    CASE 
        WHEN conflict_dup_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_duplicate
 
FROM ranked
-- Deduplication step: Filter to keep only one record for every exact match (Rule 1)
WHERE row_num = 1;