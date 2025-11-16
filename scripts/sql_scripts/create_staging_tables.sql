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
CREATE TABLE staging.stg_line_items_products();

DROP TABLE IF EXISTS staging.stg_line_items_prices;
CREATE TABLE staging.stg_line_items_prices();

--Orders
DROP TABLE IF EXISTS staging.stg_orders;
CREATE TABLE staging.stg_orders();

--Order Delays
DROP TABLE IF EXISTS staging.stg_order_delays;
CREATE TABLE staging.stg_order_delays();

--Customer
DROP TABLE IF EXISTS
CREATE TABLE staging.stg_user_data()

DROP TABLE IF EXISTS ng.stg_user_job;
CREATE TABLE staging.stg_user_job();

DROP TABLE IF EXISTS staging.stg_user_credit_card;
CREATE TABLE staging.stg_user_credit_card();

--Enterprise/Merchant
DROP TABLE IF EXISTS staging.stg_merchant_data;
CREATE TABLE staging.stg_merchant_data();

DROP TABLE IF EXISTS staging.stg_order_merchant;
CREATE TABLE staging.stg_order_merchant();

--Staff
DROP TABLE IF EXISTS staging.stg_staff;
CREATE TABLE staging.stg_staff();

--Marketing
DROP TABLE IF EXISTS staging.stg_campaign;
CREATE TABLE staging.stg_campaign();

DROP TABLE IF EXISTS staging.stg_campaign_transactions;
CREATE TABLE staging.stg_campaign_transactions();