-- Load Script: Load FactOrder
-- Source Views: 
--   - staging.clean_stg_orders (order_id, user_id, transaction_date, estimated_arrival)
--   - staging.view_clean_order_merchant (order_id, merchant_id, staff_id)
--   - staging.clean_stg_order_delays (order_id, delay_in_days)
--   - staging.view_clean_campaign_transactions (order_id, campaign_id, availed)
-- Strategy: Join all order-related views, resolve dimension keys, calculate metrics

WITH order_base AS (
    SELECT
        o.order_id,
        o.user_id,
        o.transaction_date,
        o.estimated_arrival,
        om.merchant_id,
        om.staff_id,
        od.delay_in_days,
        ct.campaign_id,
        ct.availed AS availed_flag
    FROM staging.clean_stg_orders o
    LEFT JOIN staging.view_clean_order_merchant om
        ON o.order_id = om.order_id
    LEFT JOIN staging.clean_stg_order_delays od
        ON o.order_id = od.order_id
    LEFT JOIN staging.view_clean_campaign_transactions ct
        ON o.order_id = ct.order_id
    WHERE o.order_id IS NOT NULL
),
line_item_aggregates AS (
    SELECT
        order_id,
        COUNT(*) AS number_of_items,
        SUM(line_total_amount) AS order_total_amount
    FROM warehouse.FactOrderLineItem
    GROUP BY order_id
),
order_with_metrics AS (
    SELECT
        ob.*,
        COALESCE(li.number_of_items, 0) AS number_of_items,
        COALESCE(li.order_total_amount, 0.00) AS order_total_amount,
        -- Calculate estimated arrival date
        CASE
            WHEN ob.estimated_arrival IS NOT NULL AND ob.transaction_date IS NOT NULL
            THEN (ob.transaction_date::DATE + ob.estimated_arrival * INTERVAL '1 day')::DATE
            ELSE NULL
        END AS estimated_arrival_date,
        -- Calculate actual arrival date
        CASE
            WHEN ob.delay_in_days IS NOT NULL 
                 AND ob.estimated_arrival IS NOT NULL 
                 AND ob.transaction_date IS NOT NULL
            THEN (ob.transaction_date::DATE + ob.estimated_arrival * INTERVAL '1 day' + ob.delay_in_days * INTERVAL '1 day')::DATE
            WHEN ob.estimated_arrival IS NOT NULL AND ob.transaction_date IS NOT NULL
            THEN (ob.transaction_date::DATE + ob.estimated_arrival * INTERVAL '1 day')::DATE
            ELSE NULL
        END AS actual_arrival_date
    FROM order_base ob
    LEFT JOIN line_item_aggregates li
        ON ob.order_id = li.order_id
),
order_with_dimensions AS (
    SELECT
        owm.*,
        dc.customer_key,
        dm.merchant_key,
        ds.staff_key,
        dc_campaign.campaign_key,
        -- Date keys
        TO_NUMBER(TO_CHAR(owm.transaction_date::DATE, 'YYYYMMDD'), '99999999') AS transaction_date_key,
        CASE
            WHEN owm.estimated_arrival_date IS NOT NULL
            THEN TO_NUMBER(TO_CHAR(owm.estimated_arrival_date, 'YYYYMMDD'), '99999999')
            ELSE NULL
        END AS estimated_arrival_date_key,
        CASE
            WHEN owm.actual_arrival_date IS NOT NULL
            THEN TO_NUMBER(TO_CHAR(owm.actual_arrival_date, 'YYYYMMDD'), '99999999')
            ELSE NULL
        END AS actual_arrival_date_key,
        -- Discount amount (if campaign availed)
        CASE
            WHEN owm.availed_flag = TRUE AND dc_campaign.discount_value IS NOT NULL
            THEN dc_campaign.discount_value
            ELSE 0.00
        END AS discount_amount
    FROM order_with_metrics owm
    LEFT JOIN warehouse.DimCustomer dc
        ON TRIM(owm.user_id) = TRIM(dc.user_id)
    LEFT JOIN warehouse.DimMerchant dm
        ON UPPER(TRIM(owm.merchant_id)) = UPPER(TRIM(dm.merchant_id))
    LEFT JOIN warehouse.DimStaff ds
        ON UPPER(TRIM(owm.staff_id)) = UPPER(TRIM(ds.staff_id))
    LEFT JOIN warehouse.DimCampaign dc_campaign
        ON UPPER(TRIM(owm.campaign_id)) = UPPER(TRIM(dc_campaign.campaign_id))
    WHERE dc.customer_key IS NOT NULL  -- Customer is required
)
INSERT INTO warehouse.FactOrder (
    order_id,
    customer_key,
    merchant_key,
    staff_key,
    transaction_date_key,
    estimated_arrival_date_key,
    actual_arrival_date_key,
    campaign_key,
    availed_flag,
    order_total_amount,
    discount_amount,
    net_order_amount,
    number_of_items,
    delay_in_days
)
SELECT
    order_id,
    customer_key,
    merchant_key,
    staff_key,
    transaction_date_key,
    estimated_arrival_date_key,
    actual_arrival_date_key,
    campaign_key,
    COALESCE(availed_flag, FALSE) AS availed_flag,
    order_total_amount,
    discount_amount,
    (order_total_amount - discount_amount) AS net_order_amount,
    number_of_items,
    delay_in_days
FROM order_with_dimensions
WHERE transaction_date_key IS NOT NULL  -- Transaction date is required

ON CONFLICT (order_id)
DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    merchant_key = EXCLUDED.merchant_key,
    staff_key = EXCLUDED.staff_key,
    transaction_date_key = EXCLUDED.transaction_date_key,
    estimated_arrival_date_key = EXCLUDED.estimated_arrival_date_key,
    actual_arrival_date_key = EXCLUDED.actual_arrival_date_key,
    campaign_key = EXCLUDED.campaign_key,
    availed_flag = EXCLUDED.availed_flag,
    order_total_amount = EXCLUDED.order_total_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_order_amount = EXCLUDED.net_order_amount,
    number_of_items = EXCLUDED.number_of_items,
    delay_in_days = EXCLUDED.delay_in_days;

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.FactOrder;

-- Check data loaded
-- SELECT * FROM warehouse.FactOrder ORDER BY order_id LIMIT 10;

