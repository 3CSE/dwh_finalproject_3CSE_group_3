WITH source_data AS (
    SELECT
        product_bk,
        product_id,
        product_name,
        product_type,
        price,
        ingestion_date,
        source_filename,
        MD5(
            COALESCE(product_name, '') ||
            COALESCE(product_type, '') ||
            COALESCE(CAST(price AS TEXT), '')
        ) AS product_attribute_hash
    FROM staging.view_clean_product_list
),

latest_source_record AS (
    SELECT
        product_bk,
        product_id,
        product_name,
        product_type,
        price,
        product_attribute_hash
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_bk
                ORDER BY 
                    ingestion_date DESC,
                    source_filename DESC
            ) AS rn
        FROM source_data
    ) AS t
    WHERE rn = 1
),

changes AS (
    SELECT
        s.*,
        d.product_key AS dim_key,
        d.product_attribute_hash AS dim_hash,
        d.product_bk AS existing_bk,
        (s.product_attribute_hash IS DISTINCT FROM d.product_attribute_hash) AS is_data_changed
    FROM latest_source_record s
    LEFT JOIN warehouse.DimProduct d 
        ON s.product_bk = d.product_bk
        AND d.is_current = TRUE
),

deactivate_old AS (
    UPDATE warehouse.DimProduct d
    SET 
        is_current = FALSE,
        end_date = CURRENT_TIMESTAMP - INTERVAL '1 second'
    FROM changes c
    WHERE d.product_key = c.dim_key
      AND c.is_data_changed = TRUE
    RETURNING d.product_key
)

INSERT INTO warehouse.DimProduct (
    product_bk, product_id, is_current, effective_date, end_date,
    product_name, product_type, price, product_attribute_hash
)
SELECT
    product_bk, product_id, TRUE, CURRENT_TIMESTAMP, NULL,
    product_name, product_type, price, product_attribute_hash
FROM changes
WHERE
    existing_bk IS NULL
    OR
    is_data_changed = TRUE;