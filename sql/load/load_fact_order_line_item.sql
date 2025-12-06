-- Load Script: Load FactOrderLineItem
-- Source Views: staging.view_clean_line_items_products, staging.view_clean_line_items_prices
-- Strategy: Match products with prices/quantities by position within each order_id
-- Note: Since prices table doesn't have product_id, we match by row number within order

WITH products_ranked AS (
    SELECT
        order_id,
        product_id,
        product_name,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY product_id, product_name
        ) AS product_seq
    FROM staging.view_clean_line_items_products
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL
),
prices_ranked AS (
    SELECT
        order_id,
        price AS unit_price,
        quantity,
        line_total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY price DESC, quantity DESC
        ) AS price_seq
    FROM staging.view_clean_line_items_prices
    WHERE order_id IS NOT NULL
      AND quantity IS NOT NULL
      AND quantity > 0
),
line_items_combined AS (
    SELECT
        pr.order_id,
        pr.product_id,
        pr.product_name,
        lp.unit_price,
        lp.quantity,
        lp.line_total_amount
    FROM products_ranked pr
    INNER JOIN prices_ranked lp
        ON pr.order_id = lp.order_id
        AND pr.product_seq = lp.price_seq
),
ranked_line_items AS (
    SELECT
        lc.*,
        ROW_NUMBER() OVER (
            PARTITION BY lc.order_id, lc.product_id
            ORDER BY lc.unit_price DESC, lc.quantity DESC
        ) AS row_num
    FROM line_items_combined lc
)
INSERT INTO warehouse.FactOrderLineItem (
    order_id,
    product_key,
    quantity,
    unit_price,
    line_total_amount
)
SELECT
    rli.order_id,
    dp.product_key,
    rli.quantity,
    rli.unit_price,
    rli.line_total_amount
FROM ranked_line_items rli
INNER JOIN warehouse.DimProduct dp
    ON UPPER(TRIM(rli.product_id)) = UPPER(TRIM(dp.product_id))
WHERE rli.row_num = 1
  AND dp.product_key IS NOT NULL

ON CONFLICT (order_id, product_key)
DO UPDATE SET
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total_amount = EXCLUDED.line_total_amount;

-- Optional: Check how many rows loaded
-- SELECT COUNT(*) FROM warehouse.FactOrderLineItem;

-- Check data loaded
-- SELECT * FROM warehouse.FactOrderLineItem ORDER BY line_item_key LIMIT 10;

