drop table dds.dm_couriers;
CREATE TABLE dds.dm_couriers (
	id int4 NOT null,
	courier_id varchar NOT null,
	courier_name varchar NOT null,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

drop table dds.dm_deliveries;
CREATE TABLE dds.dm_deliveries (
	id int4 NOT null,
	order_id varchar NOT null,
	delivery_id varchar NOT null,
	courier_id varchar NOT null,
	order_ts timestamp not null,
	delivery_ts timestamp not null,
	rate float not null,
	tip_sum float not null,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id)
);
