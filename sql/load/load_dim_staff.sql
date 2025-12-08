WITH source_data AS (
    SELECT
        staff_bk,
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
        source_filename,
        MD5(
            COALESCE(name, '') ||
            COALESCE(job_level, '') ||
            COALESCE(street, '') ||
            COALESCE(city, '') ||
            COALESCE(state, '') ||
            COALESCE(country, '') ||
            COALESCE(contact_number, '')
        ) AS staff_attribute_hash
    FROM staging.view_clean_staff
),

latest_source_record AS (
    SELECT
        staff_bk,
        staff_id,
        name,
        job_level,
        street,
        city,
        state,
        country,
        contact_number,
        creation_date,
        staff_attribute_hash
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY staff_bk
                ORDER BY 
                    ingestion_date DESC,
                    source_filename DESC -- Secondary tie-breaker for deterministic sorting
            ) AS rn
        FROM source_data
    ) AS t
    WHERE rn = 1
),

changes AS (
    SELECT
        s.*,
        d.staff_key AS dim_key,
        d.staff_attribute_hash AS dim_hash,
        d.staff_bk AS existing_bk,
        (s.staff_attribute_hash IS DISTINCT FROM d.staff_attribute_hash) AS is_data_changed
    FROM latest_source_record s
    LEFT JOIN warehouse.DimStaff d 
        ON s.staff_bk = d.staff_bk
        AND d.is_current = TRUE
),

deactivate_old AS (
    UPDATE warehouse.DimStaff d
    SET 
        is_current = FALSE,
        end_date = CURRENT_TIMESTAMP - INTERVAL '1 second'
    FROM changes c
    WHERE d.staff_key = c.dim_key
      AND c.is_data_changed = TRUE
    RETURNING d.staff_key
)

INSERT INTO warehouse.DimStaff (
    staff_bk, staff_id, is_current, effective_date, end_date,
    name, job_level, street, city, state, country, contact_number, creation_date,
    staff_attribute_hash
)
SELECT
    staff_bk, staff_id, TRUE, CURRENT_TIMESTAMP, NULL,
    name, job_level, street, city, state, country, contact_number, creation_date,
    staff_attribute_hash
FROM changes
WHERE
    existing_bk IS NULL
    OR
    is_data_changed = TRUE;