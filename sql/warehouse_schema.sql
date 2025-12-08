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
    merchant_bk TEXT NOT NULL,
    merchant_id TEXT,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
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
    staff_id TEXT UNIQUE,
    name TEXT,
    job_level TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    contact_number TEXT,
    creation_date TIMESTAMP
);

-- DimCustomer
DROP TABLE IF EXISTS warehouse.DimCustomer;
CREATE TABLE warehouse.DimCustomer (
    customer_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_bk TEXT NOT NULL,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_date TIMESTAMP,
    user_id TEXT,
    name TEXT,
    gender TEXT,
    birthdate TIMESTAMP,
    creation_date TIMESTAMP,
    street TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    device_address TEXT,
    user_type TEXT,
    job_title TEXT,
    job_level TEXT,
    credit_card_number TEXT,
    issuing_bank TEXT
);

-- DimCampaign
DROP TABLE IF EXISTS warehouse.DimCampaign;
CREATE TABLE warehouse.DimCampaign (
    campaign_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    campaign_id TEXT,
    campaign_name TEXT,
    campaign_description TEXT,
    discount_value NUMERIC(18,2)
);

-- DimDate
DROP TABLE IF EXISTS warehouse.DimDate;
CREATE TABLE warehouse.DimDate (
    date_key INT PRIMARY KEY,
    full_date TIMESTAMP,
    day INT,
    month INT,
    year INT,
    month_name TEXT,
    quarter INT,
    day_of_week INT
);

-- FactOrder
DROP TABLE IF EXISTS warehouse.FactOrder;
CREATE TABLE warehouse.FactOrder (
    order_id TEXT PRIMARY KEY,
    customer_key INT,
    merchant_key INT,
    staff_key INT,
    transaction_date_key INT,
    estimated_arrival_date_key INT,
    actual_arrival_date_key INT,
    campaign_key INT,
    availed_flag BOOLEAN,
    order_total_amount NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    net_order_amount NUMERIC(18,2),
    number_of_items INT,
    delay_in_days INT,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_key) REFERENCES warehouse.DimCustomer(customer_key),
    CONSTRAINT fk_order_merchant
        FOREIGN KEY (merchant_key) REFERENCES warehouse.DimMerchant(merchant_key),
    CONSTRAINT fk_order_staff
        FOREIGN KEY (staff_key) REFERENCES warehouse.DimStaff(staff_key),
    CONSTRAINT fk_order_campaign
        FOREIGN KEY (campaign_key) REFERENCES warehouse.DimCampaign(campaign_key),
    CONSTRAINT fk_order_transaction_date
        FOREIGN KEY (transaction_date_key) REFERENCES warehouse.DimDate(date_key),
    CONSTRAINT fk_order_estimated_arrival_date
        FOREIGN KEY (estimated_arrival_date_key) REFERENCES warehouse.DimDate(date_key),
    CONSTRAINT fk_order_actual_arrival_date
        FOREIGN KEY (actual_arrival_date_key) REFERENCES warehouse.DimDate(date_key)
);

--FactOrderLineItem
DROP TABLE IF EXISTS warehouse.FactOrderLineItem;
CREATE TABLE warehouse.FactOrderLineItem (
    line_item_key INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    order_id TEXT,
    product_key INT,
    quantity INT,
    unit_price NUMERIC(18,2),
    line_total_amount NUMERIC(18,2), 
    CONSTRAINT fk_lineitem_product
    FOREIGN KEY (product_key) REFERENCES warehouse.DimProduct(product_key),
    UNIQUE (order_id, product_key)
);