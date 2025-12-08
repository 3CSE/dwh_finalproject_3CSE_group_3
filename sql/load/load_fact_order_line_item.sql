-- Load Script: Load FactOrderLineItem
-- Strategy: Match items by (order_id, product_id) and perform an Upsert.
-- Note: Assumes staging views (view_clean_line_items_products and view_clean_line_items_prices) 
-- can be joined or aggregated to produce a single, unique line item record identified by (order_id, product_id).

WITH line_items_combined AS (
    -- Step 1: Combine product data and price data. 
    -- We assume the cleaning/joining step (not shown here) ensures that for a given order_id, 
    -- the product_id accurately links to its corresponding price/quantity.
    SELECT
        pp.order_id,
        pp.product_id,
        pp.product_name,
        lp.unit_price,
        lp.quantity,
        lp.line_total_amount
    FROM staging.view_clean_line_items_products pp
    INNER JOIN staging.view_clean_line_items_prices lp
        -- The join should now happen on the unique identifiers available in both source views.
        -- Assuming product_id has been correctly propagated to the price view/data structure.
        ON pp.order_id = lp.order_id AND pp.product_id = lp.product_id
    WHERE pp.order_id IS NOT NULL
      AND pp.product_id IS NOT NULL
      AND lp.quantity IS NOT NULL
      AND lp.quantity > 0
),
final_line_items AS (
    -- Step 2: Deduplicate in case of residual duplicates in the combined source
    -- We take the latest/most recent price data if duplicates still exist
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
                ORDER BY unit_price DESC, quantity DESC -- Arbitrary tie-breaker for duplicates
            ) AS rn
        FROM line_items_combined
    ) AS t
    WHERE rn = 1
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