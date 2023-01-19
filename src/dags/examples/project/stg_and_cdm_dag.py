from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

with DAG(
        'delivery_data_stg_to_dds_cdm',
        description='Обработка апишных json -> dds -> cdm',
        start_date=pendulum.datetime(2022, 5, 5, tz="UTC"), 
        schedule_interval='15 * * * *',  # Расписание - загрузка 1 раз в час - в 15 минут
        catchup=False,
        tags=['sprint5_project', 'dds', 'cdm'], 
        is_paused_upon_creation=True 
) as dag:

    reset_tables = PostgresOperator(
        task_id='reset_tables',
        postgres_conn_id=postgres_conn_id,
        sql="sql/stg_reset.sql")

    load_couriers_and_deliveries = PostgresOperator(
        task_id='load_couriers_and_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="sql/dds.sql")

    reset_report = PostgresOperator(
        task_id='reset_report',
        postgres_conn_id=postgres_conn_id,
        sql="sql/cdm_reset.sql")

    load_report = PostgresOperator(
        task_id='load_report',
        postgres_conn_id=postgres_conn_id,
        sql="sql/load_report_cdm.sql")


    reset_tables >> load_couriers_and_deliveries >> reset_report >> load_report