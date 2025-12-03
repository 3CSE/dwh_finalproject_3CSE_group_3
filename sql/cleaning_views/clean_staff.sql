-- View: Clean Staff Dimension
-- Source Table: staging.stg_staff
-- Target Usage: Populates warehouse.DimStaff

CREATE OR REPLACE VIEW staging.view_clean_staff AS
SELECT
    -- 1. Identity Columns
    TRIM(UPPER(staff_id)) AS staff_id,

    -- 2. Descriptive Fields
    -- Name
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

    -- Job Level
    INITCAP(TRIM(job_level)) AS job_level,
    
    -- Address Fields
    INITCAP(TRIM(street)) AS street, 
    INITCAP(TRIM(city)) AS city,
    INITCAP(TRIM(state)) AS state,
    INITCAP(TRIM(country)) AS country,

    -- 3. Metric/Attribute Transformation: Contact Number
    REGEXP_REPLACE(contact_number, '[^0-9]', '', 'g') AS contact_number, 

    -- 4. Metadata
    creation_date,
    source_filename,
    ingestion_date

FROM 
    staging.stg_staff;