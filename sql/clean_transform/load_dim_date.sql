WITH 
-- Determine overall min and max date across all staging tables
date_ranges AS (
    SELECT MIN(d) AS min_date, MAX(d) AS max_date
    FROM (
        -- Orders transaction_date
        SELECT MIN(transaction_date)::DATE AS d FROM staging.stg_orders
        UNION ALL
        SELECT MAX(transaction_date)::DATE AS d FROM staging.stg_orders

        -- Orders estimated_arrival ("10days" format)
        UNION ALL
        SELECT MIN(
            transaction_date::DATE
            + COALESCE(REGEXP_REPLACE(LOWER(TRIM(estimated_arrival)), '[^0-9]', '', 'g')::INT, 0) * INTERVAL '1 day'
        ) AS d
        FROM staging.stg_orders
        UNION ALL
        SELECT MAX(
            transaction_date::DATE
            + COALESCE(REGEXP_REPLACE(LOWER(TRIM(estimated_arrival)), '[^0-9]', '', 'g')::INT, 0) * INTERVAL '1 day'
        ) AS d
        FROM staging.stg_orders

        -- User creation dates
        UNION ALL
        SELECT MIN(creation_date)::DATE FROM staging.stg_user_data
        UNION ALL
        SELECT MAX(creation_date)::DATE FROM staging.stg_user_data

        -- Merchant creation dates
        UNION ALL
        SELECT MIN(creation_date)::DATE FROM staging.stg_merchant_data
        UNION ALL
        SELECT MAX(creation_date)::DATE FROM staging.stg_merchant_data

        -- Staff creation dates
        UNION ALL
        SELECT MIN(creation_date)::DATE FROM staging.stg_staff
        UNION ALL
        SELECT MAX(creation_date)::DATE FROM staging.stg_staff

        -- Campaign transactions
        UNION ALL
        SELECT MIN(transaction_date)::DATE FROM staging.stg_campaign_transactions
        UNION ALL
        SELECT MAX(transaction_date)::DATE FROM staging.stg_campaign_transactions
    ) AS all_dates
),

-- Generate continuous date series
date_series AS (
    SELECT generate_series(min_date, max_date, interval '1 day')::DATE AS full_date
    FROM date_ranges
)

-- Transform date attributes and load into DimDate
INSERT INTO warehouse.DimDate (
    date_key,
    full_date,
    day,
    month,
    year,
    month_name,
    quarter,
    day_of_week
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'), '99999999') AS date_key,
    full_date::TIMESTAMP AS full_date,
    EXTRACT(DAY FROM full_date)::INT AS day,
    EXTRACT(MONTH FROM full_date)::INT AS month,
    EXTRACT(YEAR FROM full_date)::INT AS year,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(QUARTER FROM full_date)::INT AS quarter,
    EXTRACT(ISODOW FROM full_date)::INT AS day_of_week  -- 1=Mon, 7=Sun
FROM date_series
ON CONFLICT (date_key)
DO NOTHING;  -- safe reload, skip duplicates

-- Optional: verify loaded data
-- SELECT COUNT(*) AS total_dates FROM warehouse.DimDate;
-- SELECT * FROM warehouse.DimDate LIMIT 10;