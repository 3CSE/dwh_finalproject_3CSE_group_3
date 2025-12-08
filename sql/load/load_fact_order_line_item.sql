-- Load Script: Load FactOrderLineItem
-- Strategy: Use Positional Matching (ROW_NUMBER) with a LEFT JOIN to prioritize Product IDs.
-- NOTE: Due to source system constraints (product_id missing in prices view), 
-- we LEFT JOIN products to prices based on (order_id, position) to ensure we keep all product references.

WITH products_ranked AS (
    -- Rank products within each order_id to enable positional joining (the driver set)
    SELECT
        order_id,
        product_id,
        product_name,
        -- Use consistent ordering to ensure reproducible results for the positional key
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY product_id, product_name
        ) AS product_seq
    FROM staging.view_clean_line_items_products
    WHERE order_id IS NOT NULL
      AND product_id IS NOT NULL
),
prices_ranked AS (
    -- Rank prices/quantities within each order_id (the optional set)
    SELECT
        order_id,
        price AS unit_price,
        quantity,
        line_total_amount,
        -- Use a deterministic ordering for positional matching
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
    -- Step 1: LEFT JOIN to keep ALL products and assign prices where available using position.
    SELECT
        pr.order_id,
        pr.product_id,
        pr.product_name,
        lp.unit_price,
        lp.quantity,
        lp.line_total_amount
    FROM products_ranked pr
    -- LEFT JOIN is CRITICAL here to keep all products
    LEFT JOIN prices_ranked lp
        ON pr.order_id = lp.order_id
        AND pr.product_seq = lp.price_seq
),
final_line_items AS (
    -- Step 2: Final deduplication and selection of the best record
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
                -- Arbitrary tie-breaker if duplicates somehow remain
                ORDER BY unit_price DESC NULLS LAST, quantity DESC NULLS LAST 
            ) AS rn
        FROM line_items_combined
    ) AS t
    WHERE rn = 1
    -- Filter out line items where we couldn't match a price OR quantity, as they cannot form a valid fact record.
    AND quantity IS NOT NULL
)

-- Step 3: Insert into FactOrderLineItem using the Surrogate Key (product_key)
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
-- Join on DimProduct using the Business Key (product_id) to get the Surrogate Key
INNER JOIN warehouse.DimProduct dp
    -- Match the active (is_current = TRUE) dimension record
    ON fli.product_id = dp.product_id
    AND dp.is_current = TRUE
WHERE dp.product_key IS NOT NULL

-- Conflict resolution: Upsert based on the unique index (order_id, product_key)
ON CONFLICT (order_id, product_key)
DO UPDATE SET
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    line_total_amount = EXCLUDED.line_total_amount;