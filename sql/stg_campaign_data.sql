create table stg_campaign_data (
	raw_data TEXT,
	ingested_at TIMESTAMP DEFAULT NOW()
);