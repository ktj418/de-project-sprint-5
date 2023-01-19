from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class RestObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class RestsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_rests(self, rest_threshold: int, limit: int) -> List[RestObj]:
        with self._db.client().cursor(row_factory=class_row(RestObj)) as cur:
            cur.execute(
                """
                    select distinct id,
	                    (object_value::JSON->>'_id') as restaurant_id
	                    , (object_value::JSON->>'name') as restaurant_name
	                    , (object_value::JSON->>'update_ts') as active_from,
	                    '2099-12-31 00:00:00.000' as active_to
                    from stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    LIMIT %(limit)s;
                """, {
                    "threshold": rest_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestDestRepository:

    def insert_rest(self, conn: Connection, rest: RestObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        id = EXCLUDED.id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "id": rest.id,
                    "restaurant_id": rest.restaurant_id,
                    "restaurant_name": rest.restaurant_name,
                    "active_from": rest.active_from,
                    "active_to": rest.active_to
                },
            )


class RestaurantLoader:
    WF_KEY = "restaurants_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestsOriginRepository(pg_origin)
        self.stg = RestDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_rests(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_rests(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.stg.insert_rest(conn, restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    
