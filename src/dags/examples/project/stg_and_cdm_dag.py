import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pendulum
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.types import String
from sqlalchemy.sql import text
#import time
#from datetime import datetime, timedelta

postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'
business_dt = '{{ yesterday_ds }}' # за предыдущий день

def deliveries_to_dds(business_dt):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    sql = text(
        """
            insert into dds.dm_deliveries(order_id, delivery_id, courier_id, order_ts, delivery_ts, rate, tip_sum)
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
            select *
            from tab
            where cast(order_ts as date) = :business_dt;
        """
                ).bindparams(bindparam("business_dt", String))
    engine.execute(sql, business_dt=business_dt)

def load_main_report(business_dt):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    sql = text(
        """
            DELETE FROM cdm.dm_courier_ledger 
            WHERE settlement_year = extract('year' from cast(:business_dt as date))
                and settlement_month = extract('month' from cast(:business_dt as date));
            insert into cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
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
            select *
            from calc
            where settlement_year = extract('year' from cast(:business_dt as date))
                and settlement_month = extract('month' from cast(:business_dt as date));;
        """
                ).bindparams(bindparam("business_dt", String))
    engine.execute(sql, business_dt=business_dt)

with DAG(
        'delivery_data_stg_to_dds_cdm',
        description='Обработка апишных json -> dds -> cdm',
        start_date=pendulum.datetime(2022, 12, 25, tz="UTC"), 
        schedule_interval='0 1 * * *',  # Расписание - загрузка в 1 час ночи
        catchup=True,
        tags=['sprint5_project', 'dds', 'cdm'], 
        is_paused_upon_creation=True 
) as dag:

    load_couriers = PostgresOperator(
        task_id='load_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="sql/dm_add_couriers.sql")

    deliveries_to_dds = PythonOperator(
            task_id='deliveries_to_dds',
            python_callable=deliveries_to_dds,
            do_xcom_push = False,
            op_kwargs={
                'business_dt':business_dt
                })

    load_main_report = PythonOperator(
            task_id='load_main_report',
            python_callable=load_main_report,
            do_xcom_push = False,
            op_kwargs={
                'business_dt':business_dt
                })


    load_couriers >> deliveries_to_dds >> load_main_report