/*
* ДЛЯ СПРАВКИ
*/
drop table dds.dm_couriers;
CREATE TABLE dds.dm_couriers (
	id serial4 NOT null,
	courier_id varchar NOT null,
	courier_name varchar NOT null,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

drop table dds.dm_deliveries;
CREATE TABLE dds.dm_deliveries (
	id serial4 NOT null,
	order_id varchar NOT null,
	delivery_id varchar NOT null,
	courier_id varchar NOT null,
	order_ts timestamp not null,
	delivery_ts timestamp not null,
	rate float not null,
	tip_sum float not null,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id)
);

DROP TABLE cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
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
	CONSTRAINT dm_couriers_report_pkey PRIMARY KEY (id),
	CONSTRAINT unique_set UNIQUE (courier_id, settlement_year, settlement_month)
);

