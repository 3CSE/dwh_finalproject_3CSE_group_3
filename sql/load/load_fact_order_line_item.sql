-- Load Script: Load FactOrderLineItem
-- Strategy: Use Positional Matching (ROW_NUMBER) with a LEFT JOIN to prioritize Product IDs.

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
    LEFT JOIN prices_ranked lp
        ON pr.order_id = lp.order_id
        AND pr.product_seq = lp.price_seq
),
final_line_items AS (
    SELECT
        order_id,
        product_id,
        unit_price,
        quantity,
        line_total_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id, product_id
                ORDER BY unit_price DESC NULLS LAST, quantity DESC NULLS LAST 
            ) AS rn
        FROM line_items_combined
    ) AS t
    WHERE rn = 1
    AND quantity IS NOT NULL
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
FROM final_line_items fli
INNER JOIN warehouse.DimProduct dp
    ON fli.product_id = dp.product_id
    AND dp.is_current = TRUE
WHERE dp.product_key IS NOT NULL

ON CONFLICT (order_id, product_key)
DO UPDATE SET
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total_amount = EXCLUDED.line_total_amount;