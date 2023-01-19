import requests
import pandas as pd
from typing import List

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pendulum


postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

headers = {
    'X-Nickname': 'kt4ja418', 
    'X-Cohort': '7', 
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}

def get_couriers(url, pg_table, pg_schema, offset=0, headers = headers):
    couriers_df = pd.DataFrame(columns = ['id', 'courier_data'])
    productivity_counter = 1
    while productivity_counter > 0:
        full_url = url + str(offset)
        df = pd.DataFrame({'courier_data':requests.get(full_url, headers=headers).json()}).reset_index(level=0)
        df.columns = ['id', 'courier_data']
        offset+=50
        couriers_df = couriers_df.append(df)
        productivity_counter = len(df)
    couriers_df = couriers_df.reset_index()
    couriers_df['id'] = couriers_df.index
    couriers_df = couriers_df[['id', 'courier_data']].copy()
    couriers_df['courier_data'] = couriers_df['courier_data'].astype(str).replace('\'', '"')
    
    # загрузка в стейджинг
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = couriers_df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')

def get_deliveries(url, pg_table, pg_schema, offset=0, headers = headers):
    deliveries_df = pd.DataFrame(columns = ['id', 'delivery_data'])
    productivity_counter = 1
    while productivity_counter > 0:
        full_url = url + str(offset)
        df = pd.DataFrame({'delivery_data':requests.get(full_url, headers=headers).json()}).reset_index(level=0)
        df.columns = ['id', 'delivery_data']
        offset+=50
        deliveries_df = pd.concat([deliveries_df, df])
        productivity_counter = len(df)
    deliveries_df = deliveries_df.reset_index()
    deliveries_df['id'] = deliveries_df.index
    deliveries_df = deliveries_df[['id', 'delivery_data']].copy()
    deliveries_df['delivery_data'] = deliveries_df['delivery_data'].astype(str).replace('\'', '"')

    # загрузка в стейджинг
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = deliveries_df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')

with DAG(
        'get_api_data_to_staging',
        description='Выгрузка сырых данных из источника',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
        schedule_interval='0 * * * *',  # Расписание - загрузка 1 раз в час
        catchup=False,
        tags=['sprint5_project', 'stg', 'api'], 
        is_paused_upon_creation=True 
) as dag:

    reset_couriers = PostgresOperator(
        task_id='reset_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="sql/reset_couriers.sql")

    reset_deliveries = PostgresOperator(
        task_id='reset_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="sql/reset_deliveries.sql")

    generate_couriers = PythonOperator(
        task_id='generate_couriers',
        python_callable=get_couriers,
        do_xcom_push = False,
        op_kwargs={
            'url': 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=_id&sort_direction=asc&limit=50&offset=', 
            'pg_table':'api_couriers', 
            'pg_schema':'stg'
            })

    generate_deliveries = PythonOperator(
        task_id='generate_deliveries',
        python_callable=get_deliveries,
        do_xcom_push = False,
        op_kwargs={
            'url': 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=order_id&sort_direction=asc&limit=50&offset=', 
            'pg_table':'api_deliveries', 
            'pg_schema':'stg'
            })



    reset_couriers >> generate_couriers >> reset_deliveries >> generate_deliveries 