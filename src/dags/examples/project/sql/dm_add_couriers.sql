insert into dds.dm_couriers(courier_id, courier_name)
with cte as 
    (select 
        replace(courier_data, '''', '"')::JSON->>'_id' as courier_id, 
        replace(courier_data, '''', '"')::JSON->>'name' as courier_name
    from stg.api_couriers)
select *
from cte
-- проверка на предмет того, есть ли уже такой курьер в dds.dm_couriers
where courier_id not in (select courier_id from dds.dm_couriers);