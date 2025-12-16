-- Load Script: Load FactOrderLineItem
-- Strategy: Use Positional Matching (ROW_NUMBER) with a LEFT JOIN to prioritize Product IDs.

BEGIN;
-- This prevents duplicates when rerunning the script.
DELETE FROM warehouse.FactOrderLineItem 
WHERE order_id IN (
    SELECT DISTINCT order_id 
    FROM staging.view_clean_line_items_products
    WHERE order_id IS NOT NULL
);

WITH products_ranked AS (
    SELECT
        order_id,
        product_id,
        product_name,
        line_item_seq 
    FROM staging.view_clean_line_items_products
    WHERE order_id IS NOT NULL AND product_id IS NOT NULL
),
prices_ranked AS (
    SELECT
        order_id,
        price AS unit_price,
        quantity,
        line_total_amount,
        line_item_seq 
    FROM staging.view_clean_line_items_prices
    WHERE order_id IS NOT NULL AND quantity > 0
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
        AND pr.line_item_seq = lp.line_item_seq 
)


INSERT INTO warehouse.FactOrderLineItem (
    order_id,
    product_key,
    quantity,
    unit_price,
    line_total_amount
)
SELECT
    fli.order_id,
    dp.product_key,
    fli.quantity,
    fli.unit_price,
    fli.line_total_amount
FROM line_items_combined fli
INNER JOIN warehouse.DimProduct dp
    ON fli.product_id = dp.product_id
    AND dp.is_current = TRUE
WHERE dp.product_key IS NOT NULL;

COMMIT; 

