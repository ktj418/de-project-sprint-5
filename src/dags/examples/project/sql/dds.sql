insert into dds.dm_couriers(id, courier_id, courier_name)
with cte as 
    (select 
        replace(courier_data, '''', '"')::JSON->>'_id' as courier_id, 
        replace(courier_data, '''', '"')::JSON->>'name' as courier_name
    from stg.api_couriers)
select row_number() over() as id, *
from cte;

insert into dds.dm_deliveries(id, order_id, delivery_id, courier_id, order_ts, delivery_ts, rate, tip_sum)
with raw as 
	(select replace(delivery_data, '''', '"') rawcol
	from stg.api_deliveries),
tab as 
	(select 
	rawcol::json->>'order_id' as order_id
	, rawcol::json->>'delivery_id' as delivery_id
	, rawcol::json->>'courier_id' as courier_id
	, (rawcol::json->>'order_ts')::timestamp as order_ts
	, (rawcol::json->>'delivery_ts')::timestamp as delivery_ts
	, (rawcol::json->>'rate')::float as rate
	, (rawcol::json->>'tip_sum')::float as tip_sum
	from raw)
select row_number() over() as id, *
from tab;