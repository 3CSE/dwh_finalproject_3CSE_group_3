CREATE SCHEMA IF NOT EXISTS warehouse;

-- DimProduct
DROP TABLE IF EXISTS warehouse.DimProduct;
CREATE TABLE warehouse.DimProduct (
    product_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    product_id TEXT,
    product_name TEXT,
    product_type TEXT,
    price NUMERIC(18,2)
);

-- DimMerchant
DROP TABLE IF EXISTS warehouse.DimMerchant;
CREATE TABLE warehouse.DimMerchant (
    merchant_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    merchant_id TEXT,
    name TEXT,
    contact_number TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    creation_date TIMESTAMP
);

-- DimStaff
DROP TABLE IF EXISTS warehouse.DimStaff;
CREATE TABLE warehouse.DimStaff (
    staff_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    staff_id TEXT,
    name TEXT,
    job_level TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    contact_number TEXT,
    creation_date TIMESTAMP
);

--FactOrderLineItem
DROP TABLE IF EXISTS warehouse.FactOrderLineItem;
CREATE TABLE warehouse.FactOrderLineItem (
    line_item_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id TEXT,
    product_key INT,
    quantity INT,
    unit_price NUMERIC(18,2),
    line_total_amount NUMERIC(18,2)
    , CONSTRAINT fk_lineitem_product
        FOREIGN KEY (product_key) REFERENCES warehouse.DimProduct(product_key)
);

