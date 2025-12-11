-- Load Script: Load FactOrder
-- Source Views: 
--   - staging.clean_stg_orders (order_id, user_id, transaction_date, estimated_arrival)
--   - staging.view_clean_order_merchant (order_id, merchant_id, staff_id)
--   - staging.clean_stg_order_delays (order_id, delay_in_days)
--   - staging.view_clean_campaign_transactions (order_id, campaign_id, availed)
-- Strategy: Join all order-related views, resolve dimension keys, calculate metrics

WITH 
orders AS (
    SELECT 
        order_id, 
        user_id, 
        estimated_arrival, 
        transaction_date,
        TO_CHAR(transaction_date, 'YYYYMMDD')::INT AS transaction_date_key,
        TO_CHAR(
            transaction_date + (estimated_arrival || ' days')::INTERVAL, 
            'YYYYMMDD'
        )::INT AS estimated_arrival_date_key
    FROM staging.clean_stg_orders
),

-- Bridge: Merchant & Staff
bridge AS (
    SELECT order_id, merchant_bk, staff_bk
    FROM staging.clean_stg_order_merchant 
),

-- Campaigns (force 1 campaign per order)
campaigns AS (
    SELECT 
        order_id, 
        campaign_id, 
        availed
    FROM (
        SELECT 
            order_id, 
            campaign_id, 
            availed,
            ROW_NUMBER() OVER (
                PARTITION BY order_id 
                ORDER BY availed DESC, ingestion_date DESC
            ) AS rn
        FROM staging.clean_stg_campaign_transactions
    ) t
    WHERE rn = 1
),

delays AS (
    SELECT order_id, delay_in_days
    FROM staging.clean_stg_order_delays
),

-- Aggregation for metrics in fact order table
metrics AS (
    SELECT 
        order_id,
        SUM(quantity) AS total_items,
        SUM(line_total_amount) AS raw_order_total
    FROM staging.view_clean_line_items_prices
    GROUP BY order_id
)

INSERT INTO warehouse.FactOrder (
    order_id,
    customer_key,
    merchant_key,
    staff_key,
    campaign_key,
    transaction_date_key,
    estimated_arrival_date_key,
    actual_arrival_date_key,
    availed_flag,
    order_total_amount,
    discount_amount,
    net_order_amount,
    number_of_items,
    delay_in_days
)
SELECT DISTINCT ON (o.order_id)
    o.order_id,
    
    COALESCE(cust.customer_key, -1) AS customer_key,
    COALESCE(merch.merchant_key, -1) AS merchant_key,
    COALESCE(stf.staff_key, -1) AS staff_key,
    COALESCE(cmp.campaign_key, -1) AS campaign_key,
    
    o.transaction_date_key,
    o.estimated_arrival_date_key,
    
    TO_CHAR(
        TO_DATE(o.estimated_arrival_date_key::TEXT, 'YYYYMMDD') + (COALESCE(d.delay_in_days, 0) || ' days')::INTERVAL, 
        'YYYYMMDD'
    )::INT AS actual_arrival_date_key,

    COALESCE(c.availed, FALSE) AS availed_flag,

    COALESCE(m.raw_order_total, 0.00) AS order_total_amount,
    
    CAST(
        CASE 
            WHEN COALESCE(c.availed, FALSE) = TRUE THEN 
                COALESCE(m.raw_order_total, 0.00) * COALESCE(cmp.discount_value, 0)
            ELSE 0.00 
        END
    AS NUMERIC(18,2)) AS discount_amount,
    
    CAST(
        COALESCE(m.raw_order_total, 0.00) - 
        CASE 
            WHEN COALESCE(c.availed, FALSE) = TRUE THEN 
                COALESCE(m.raw_order_total, 0.00) * COALESCE(cmp.discount_value, 0)
            ELSE 0.00 
        END
    AS NUMERIC(18,2)) AS net_order_amount,

    COALESCE(m.total_items, 0) AS number_of_items,
    COALESCE(d.delay_in_days, 0) AS delay_in_days

FROM orders o
LEFT JOIN bridge b ON o.order_id = b.order_id
LEFT JOIN campaigns c ON o.order_id = c.order_id
LEFT JOIN delays d ON o.order_id = d.order_id
INNER JOIN metrics m ON o.order_id = m.order_id

-- Joins to dimensions
LEFT JOIN warehouse.DimCustomer cust 
    ON o.user_id = cust.user_id 
    AND o.transaction_date >= cust.effective_date 
    AND (cust.end_date IS NULL OR o.transaction_date < cust.end_date)

LEFT JOIN warehouse.DimMerchant merch 
    ON b.merchant_bk = merch.merchant_bk 
    AND o.transaction_date >= merch.effective_date 
    AND (merch.end_date IS NULL OR o.transaction_date < merch.end_date)

LEFT JOIN warehouse.DimStaff stf 
    ON b.staff_bk = stf.staff_bk 
    AND o.transaction_date >= stf.effective_date 
    AND (stf.end_date IS NULL OR o.transaction_date < stf.end_date)

LEFT JOIN warehouse.DimCampaign cmp 
    ON c.campaign_id = cmp.campaign_id

ORDER BY o.order_id, cust.effective_date DESC, merch.effective_date DESC

ON CONFLICT (order_id) 
DO UPDATE SET
    order_total_amount = EXCLUDED.order_total_amount,
    net_order_amount = EXCLUDED.net_order_amount,
    discount_amount = EXCLUDED.discount_amount,
    number_of_items = EXCLUDED.number_of_items,
    delay_in_days = EXCLUDED.delay_in_days,
    actual_arrival_date_key = EXCLUDED.actual_arrival_date_key,
    campaign_key = EXCLUDED.campaign_key,
    availed_flag = EXCLUDED.availed_flag;

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.FactOrder;

-- Check data loaded
-- SELECT * FROM warehouse.FactOrder LIMIT 10;

-- select count(*) from warehouse.dimcampaign;
-- select count(*) from warehouse.dimcustomer;
-- select count(*) from warehouse.dimdate;
-- select count(*) from warehouse.dimmerchant;
-- select count(*) from warehouse.dimproduct;
-- select count(*) from warehouse.dimstaff;