import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.dm_users.user_loader import UserLoader
from examples.stg.dm_users.restaurant_loader import RestaurantLoader
from examples.stg.dm_users.timestamp_loader import TimeLoader
from examples.stg.dm_users.product_loader import ProdLoader
from examples.stg.dm_users.order_loader import OrderLoader
from examples.stg.dm_users.fct_loader import FctLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_stg_to_users_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

    users_dict = load_users()

    @task(task_id="dm_rests_load")
    def load_rests():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_rests()  # Вызываем функцию, которая перельет данные.

    rests_dict = load_rests()

    @task(task_id="dm_timestamps_load")
    def load_time():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = TimeLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_time()  # Вызываем функцию, которая перельет данные.

    time_dict = load_time()

    @task(task_id="dm_products_load")
    def load_prod():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProdLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_prod()  # Вызываем функцию, которая перельет данные.

    prod_dict = load_prod()

    [users_dict >> rests_dict >> time_dict] >> prod_dict # type: ignore

    @task(task_id="dm_orders_load")
    def load_order():
        # создаем экземпляр класса, в котором реализована логика.
        order_loader = OrderLoader(origin_pg_connect, dwh_pg_connect, log)
        order_loader.load_order()  # Вызываем функцию, которая перельет данные.

    order_dict = load_order()

    @task(task_id="dm_fct_load")
    def load_fct():
        # создаем экземпляр класса, в котором реализована логика.
        fct_loader = FctLoader(origin_pg_connect, dwh_pg_connect, log)
        fct_loader.load_fct()  # Вызываем функцию, которая перельет данные.

    fct_dict = load_fct()

    [users_dict >> rests_dict >> time_dict] >> prod_dict >> order_dict >> fct_dict # type: ignore


dds_users_dag = dds_stg_to_users_dag()