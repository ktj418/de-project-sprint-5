insert into cdm.dm_couriers_report(id, courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
with t as (
select 
	c.courier_id
	, c.courier_name
	, d.order_id
	, avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) as rate_avg
	, case when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.0 and o.total_sum * 0.05 >= 100 then o.total_sum * 0.05
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.0 and o.total_sum * 0.05 < 100 then 100
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.5 and o.total_sum * 0.07 >= 150 then o.total_sum * 0.07
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.5 and o.total_sum * 0.07 < 150 then o.total_sum * 0.07
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.9 and o.total_sum * 0.08 >= 175 then o.total_sum * 0.08
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) < 4.9 and o.total_sum * 0.08 < 175 then 175
		when avg(1.0 * d.rate) over (partition by c.courier_id, extract('year' from d.order_ts), extract('month' from d.order_ts)) >= 4.9 and o.total_sum * 0.1 < 200 then 200
		else o.total_sum * 0.1
		end as courier_order_sum
	, d.tip_sum
	, extract('year' from d.order_ts) as settlement_year
	, extract('month' from d.order_ts) as settlement_month
	, o.total_sum
from dds.dm_couriers c
left join dds.dm_deliveries d using (courier_id)
inner join 
	(select order_key, sum(total_sum) as total_sum
	from dds.fct_product_sales s
	left join dds.dm_orders o on s.order_id = o.id
	group by order_key) o on d.order_id = o.order_key 
),
calc as (
select 
	courier_id
	, courier_name
	, settlement_year
	, settlement_month
	, count(order_id) as orders_count
	, sum(total_sum) as orders_total_sum
	, rate_avg
	, sum(total_sum) * 0.25 as order_processing_fee
	, sum(courier_order_sum) as courier_order_sum
	, sum(tip_sum) as courier_tips_sum
	, sum(courier_order_sum) + sum(tip_sum) * 0.95 as courier_reward_sum
from t
group by courier_id, courier_name, settlement_year, settlement_month, rate_avg
)
select row_number() over() as id, *
from calc