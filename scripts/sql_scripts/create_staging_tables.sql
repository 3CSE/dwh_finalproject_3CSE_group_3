/*
	An SQL script to create staging tables in staging layer
*/

--Product
DROP TABLE IF EXISTS staging.stg_product_list;
CREATE TABLE staging.stg_product_list(
	product_id INT,
	product_name VARCHAR(50),
	product_type VARCHAR(50),
	price DECIMAL(8, 2),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);


--Line Items
DROP TABLE IF EXISTS staging.stg_line_items_products;
CREATE TABLE staging.stg_line_items_products(
	order_id VARCHAR(50),
	product_name VARCHAR(100),
	product_id INT,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_line_items_prices;
CREATE TABLE staging.stg_line_items_prices(
	order_id VARCHAR(50),
	price DECIMAL(8, 2),
	quantity INT,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Orders

DROP TABLE IF EXISTS staging.stg_orders;
CREATE TABLE staging.stg_orders(
	order_id VARCHAR(50),
	user_id INT,
	estimated_arrival INT,
	transaction_data DATE,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Order Delays
DROP TABLE IF EXISTS staging.stg_order_delays;
CREATE TABLE staging.stg_order_delays(
	order_id VARCHAR(50),
	delay_in_days INT,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Customer
DROP TABLE IF EXISTS stagin.stg_user_data;
CREATE TABLE staging.stg_user_data(
	user_id INT,
	creation_date TIMESTAMP,
	name VARCHAR(50),
	street VARCHAR(50),
	state VARCHAR(25),
	city VARCHAR(25),
	country VARCHAR(60),
	birthdate TIMESTAMP,
	gender VARCHAR(10),
	device_address VARCHAR(25),
	user_type VARCHAR(10),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

DROP TABLE IF EXISTS ng.stg_user_job;
CREATE TABLE staging.stg_user_job(
	user_id INT,
	name VARCHAR(50),
	job_title VARCHAR(25),
	job_level VARCHAR(25),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_user_credit_card;
CREATE TABLE staging.stg_user_credit_card(
	user_id INT,
	name VARCHAR(50),
	credit_card_number VARCHAR(10),
	issuing_bank VARCHAR(50),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Enterprise/Merchant
DROP TABLE IF EXISTS staging.stg_merchant_data;
CREATE TABLE staging.stg_merchant_data(
	merchant_id INT,
	creation_date DATE,
	name VARCHAR(50),
	street VARCHAR(50),
	state VARCHAR(25),
	city VARCHAR (25),
	country VARCHAR(60),
	contact_number VARCHAR(50),
	
	
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_order_merchant;
CREATE TABLE staging.stg_order_merchant(
	order_id VARCHAR(50),
	merchant_id INT,
	staff_id VARCHAR(10),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Staff
DROP TABLE IF EXISTS staging.stg_staff;
CREATE TABLE staging.stg_staff(
	staff_id VARCHAR(10),
	name VARCHAR(50),
	job_level VARCHAR(25),
	street VARCHAR(50),
	state VARCHAR(25),
	city VARCHAR(25),
	country VARCHAR(60),
	contact_number VARCHAR(25),
	creation_date TIMESTAMP,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

--Marketing
DROP TABLE IF EXISTS staging.stg_campaign;
CREATE TABLE staging.stg_campaign(
	campaign_id VARCHAR(10),
	campaign_name VARCHAR(25),
	campaign_description VARCHAR(50),
	discount DECIMAL(4, 2),
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_campaign_transactions;
CREATE TABLE staging.stg_campaign_transactions(
	transaction_date DATE,
	campaign_id VARCHAR(10),
	order_id VARCHAR(50),
	estimated_arrival INT,
	availed INT,
	-- additional metadata for data lineage/tracking
	source_filename VARCHAR (50),
	ingestion_data TIMESTAMP
);