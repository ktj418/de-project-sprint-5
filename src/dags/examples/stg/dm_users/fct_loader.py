from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum:float
    bonus_payment: float
    bonus_grant: float


class FctOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct(self, fct_threshold: int, limit: int) -> List[FctObj]:
        with self._db.client().cursor(row_factory=class_row(FctObj)) as cur:
            cur.execute(
                """
                    with raw as (select event_value::json->>'order_id' as orders, replace(replace(event_value::json->>'product_payments', '[', ''), ']', '') as js
                    from stg.bonussystem_events
                    where event_type = 'bonus_transaction'),
                    js as (select orders, '{' || replace(replace(unnest(string_to_array(js, '}, {')), '{', ''), '}', '') || '}' as js
                    --'{' || unnest(string_to_array(js, '}, {')) || '}' as js
                    from raw),
                    t as (
                    select 
                        --orders, 
                        p.id as product_id,
                        o.id as order_id,
                        --js::json->>'product_id' as product, 
                        cast(js::json->>'quantity' as int) as count, 
                        cast(js::json->>'price' as float) as price, 
                        (js::json->>'price')::float * (js::json->>'quantity')::float as total_sum, 
                        cast(js::json->>'bonus_payment' as float) as bonus_payment, 
                        cast(js::json->>'bonus_grant' as float) as bonus_grant
                    from js
                    inner join dds.dm_orders o on js.orders = o.order_key
                    left join dds.dm_products p on js::json->>'product_id' = p.product_id),
                    final as (select row_number() over() as id, *
                    from t)
                    select * from final
                    WHERE id > %(threshold)s
                    order by id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": fct_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctDestRepository:

    def insert_fct(self, conn: Connection, fct: FctObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(id)s, %(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        id = EXCLUDED.id,
                        product_id = EXCLUDED.product_id,
                        order_id = EXCLUDED.order_id,
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "id": fct.id,
                    "product_id": fct.product_id,
                    "order_id": fct.order_id,
                    "count": fct.count,
                    "price": fct.price,
                    "total_sum": fct.total_sum,
                    "bonus_payment": fct.bonus_payment,
                    "bonus_grant": fct.bonus_grant
                },
            )


class FctLoader:
    WF_KEY = "fct_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctOriginRepository(pg_origin)
        self.stg = FctDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_fct(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fct(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct rows to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct in load_queue:
                self.stg.insert_fct(conn, fct)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    
