/*
	An SQL script to create staging tables in staging layer
*/

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

--Product
DROP TABLE IF EXISTS staging.stg_product_list;
CREATE TABLE staging.stg_product_list(
	product_id TEXT,
	product_name TEXT,
	product_type TEXT,
	price DECIMAL(8, 2),
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);


--Line Items
DROP TABLE IF EXISTS staging.stg_line_items_products;
CREATE TABLE staging.stg_line_items_products(
	order_id TEXT,
	product_name TEXT,
	product_id TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_line_items_prices;
CREATE TABLE staging.stg_line_items_prices(
	order_id TEXT,
	price DECIMAL(8, 2),
	quantity TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Orders

DROP TABLE IF EXISTS staging.stg_orders;
CREATE TABLE staging.stg_orders(
	order_id TEXT,
	user_id TEXT,
	estimated_arrival TEXT,
	transaction_date DATE,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Order Delays
DROP TABLE IF EXISTS staging.stg_order_delays;
CREATE TABLE staging.stg_order_delays(
	order_id TEXT,
	delay_in_days INT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Customer
DROP TABLE IF EXISTS staging.stg_user_data;
CREATE TABLE staging.stg_user_data(
	user_id TEXT,
	creation_date TIMESTAMP,
	name TEXT,
	street TEXT,
	state TEXT,
	city TEXT,
	country TEXT,
	birthdate TIMESTAMP,
	gender TEXT,
	device_address TEXT,
	user_type TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_user_job;
CREATE TABLE staging.stg_user_job(
	user_id TEXT,
	name TEXT,
	job_title TEXT,
	job_level TEXT,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_user_credit_card;
CREATE TABLE staging.stg_user_credit_card(
	user_id TEXT,
	name TEXT,
	credit_card_number TEXT,
	issuing_bank TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Enterprise/Merchant
DROP TABLE IF EXISTS staging.stg_merchant_data;
CREATE TABLE staging.stg_merchant_data(
	merchant_id TEXT,
	creation_date DATE,
	name TEXT,
	street TEXT,
	state TEXT,
	city TEXT,
	country TEXT,
	contact_number TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_order_merchant;
CREATE TABLE staging.stg_order_merchant(
	order_id TEXT,
	merchant_id TEXT,
	staff_id TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Staff
DROP TABLE IF EXISTS staging.stg_staff;
CREATE TABLE staging.stg_staff(
	staff_id TEXT,
	name TEXT,
	job_level TEXT,
	street TEXT,
	state TEXT,
	city TEXT,
	country TEXT,
	contact_number TEXT,
	creation_date TIMESTAMP,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

--Marketing
DROP TABLE IF EXISTS staging.stg_campaign;
CREATE TABLE staging.stg_campaign(
	campaign_id TEXT,
	campaign_name TEXT,
	campaign_description TEXT,
	discount TEXT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_campaign_transactions;
CREATE TABLE staging.stg_campaign_transactions(
	transaction_date DATE,
	campaign_id TEXT,
	order_id TEXT,
	estimated_arrival INT,
	availed INT,
	-- additional metadata for data lineage/tracking
	source_filename TEXT,
	ingestion_date TIMESTAMP
);