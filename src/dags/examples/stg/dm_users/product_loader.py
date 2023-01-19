from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class ProdObj(BaseModel):
    id: str
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: str
    active_from: datetime
    active_to: datetime


class ProdOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_prod(self, prod_threshold: int) -> List[ProdObj]:
        with self._db.client().cursor(row_factory=class_row(ProdObj)) as cur:
            cur.execute(
                """
                with t as (select object_value::json->>'order_items' as js, update_ts,  object_value::json->>'restaurant' as restaurant from stg.ordersystem_orders), 
                tt as (select update_ts, replace(replace(restaurant, '{"id": "', ''), '"}', '') as restaurant, replace(replace(js::text, '[{', ''), '}]', '') as js from t),
                ttt as (select update_ts, restaurant, '{' || unnest(string_to_array(js, '}, {')) || '}' as js from tt),
                act as (select distinct js::json->>'id' as product_id, max(update_ts) over (partition by js::json->>'id') as active_from from ttt),
                fin as (select 
                    distinct r.id as restaurant_id, 
                    js::json->>'id' as product_id, 
                    js::json->>'name' as product_name, 
                    js::json->>'price' as product_price,
                    act.active_from,
                    '2099-12-31 00:00:00.000'::timestamp as active_to
                from ttt
                left join dds.dm_restaurants r on ttt.restaurant = r.restaurant_id 
                left join act on (js::json->>'id') = act.product_id),
                final_tables AS (select *, row_number() over() as id
                from fin)
                Select *
                from final_tables
                WHERE id > %(threshold)s
                order by restaurant_id, product_id;
            """, {
                    "threshold": prod_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class ProdDestRepository:

    def insert_prod(self, conn: Connection, prod: ProdObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(id, restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        id = EXCLUDED.id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;

                """,
                {
                    "id": prod.id,
                    "restaurant_id": prod.restaurant_id,
                    "product_id": prod.product_id,
                    "product_name": prod.product_name,
                    "product_price": prod.product_price,
                    "active_from": prod.active_from,
                    "active_to": prod.active_to
                },
            )

class ProdLoader:
    WF_KEY = "prod_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProdOriginRepository(pg_origin)
        self.stg = ProdDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_prod(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_prod(last_loaded)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.stg.insert_prod(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    
