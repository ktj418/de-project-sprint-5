DROP TABLE cdm.dm_couriers_report;
CREATE TABLE cdm.dm_couriers_report (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum float8 NULL,
	rate_avg float8 NOT NULL,
	order_processing_fee float8 NOT NULL,
	courier_order_sum float8 NOT NULL,
	courier_tips_sum float8 NOT NULL,
	courier_reward_sum float8 NOT NULL,
	CONSTRAINT dm_couriers_report_pkey PRIMARY KEY (id)
);