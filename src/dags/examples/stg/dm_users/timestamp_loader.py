from logging import Logger
from typing import List

from examples.stg.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class TimeObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: str
    time: str


class TimeOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_time(self, time_threshold: int) -> List[TimeObj]:
        with self._db.client().cursor(row_factory=class_row(TimeObj)) as cur:
            cur.execute(
                """
                    with t as (select id, 
	                    (object_value::JSON->>'date') as ts
                        from stg.ordersystem_orders)
                    select 
	                    id, ts, 
	                    extract('year' from ts::timestamp) as year
	                    , extract('month' from ts::timestamp) as month
	                    , extract('day' from ts::timestamp) as day
	                    , cast(ts as time)::varchar as time
	                    , cast(ts as date)::varchar as date
                        from t
                    WHERE id > %(threshold)s
                    ORDER BY id asc;
                """, {
                    "threshold": time_threshold
                }
            )
            objs = cur.fetchall()
        return objs


class TimeDestRepository:

    def insert_time(self, conn: Connection, time: TimeObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(id, ts, year, month, day, date, time)
                    VALUES (%(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                """,
                {
                    "id": time.id,
                    "ts": time.ts,
                    "year": time.year,
                    "month": time.month,
                    "day": time.day,
                    "date": time.date,
                    "time": time.time
                },
            )


class TimeLoader:
    WF_KEY = "time_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimeOriginRepository(pg_origin)
        self.stg = TimeDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_time(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            
            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_time(last_loaded)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for time in load_queue:
                self.stg.insert_time(conn, time)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
    
