-- Load Script: Load DimProduct
-- Source View: staging.view_clean_product_list
-- Strategy: Type 0/Type 6 SCD (Preserve all unique product attribute combinations)

WITH clean_source AS (
    SELECT
        product_id,
        product_name,
        product_type,
        price
    FROM staging.view_clean_product_list
)
INSERT INTO warehouse.DimProduct (
    product_id, 
    product_name, 
    product_type, 
    price
)
SELECT 
    cs.product_id, 
    cs.product_name, 
    cs.product_type, 
    cs.price
FROM clean_source cs
WHERE NOT EXISTS (
    SELECT 1
    FROM warehouse.DimProduct dp
    WHERE 
        dp.product_id = cs.product_id AND
        dp.product_name = cs.product_name AND
        dp.product_type = cs.product_type AND
        dp.price = cs.price
);