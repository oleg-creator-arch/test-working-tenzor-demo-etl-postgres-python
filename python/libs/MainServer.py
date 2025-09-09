#
#  Основной класс сервера приложений
#  Реализует обращение к источнику, обработку и запись в БД
#

import logging
import requests
import asyncpg
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class MainServer:
    def __init__(self, db_config):
        self.db_config = db_config

    # Чтение данных из Web API источника
    def getData(self, srcUrl, timeout=1000):
        try:
            res = requests.get(srcUrl, timeout=timeout / 1000)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logging.error(f"getData error: {e}")
            raise

    # Обработка прочитанных данных
    #       dataIn - массив входящих строк
    #       from_ts - метка времени начала (включая)
    #       to_ts - метка времени окончания периода (не включая)
    def prepareData(self, dataIn, from_ts, to_ts):
        if not isinstance(dataIn, list):
            return []
        grouped = {}
        for row in dataIn:
            ts = row.get("ts")
            if ts is None or ts < from_ts or ts >= to_ts:
                continue

            key = row["group_name"]
            if key not in grouped:
                grouped[key] = {
                    "group_name": key,
                    "cnt": row["cnt"],
                    "from_ts": ts,
                    "to_ts": ts,
                }
            else:
                grouped[key]["cnt"] += row["cnt"]
                grouped[key]["from_ts"] = min(grouped[key]["from_ts"], ts)
                grouped[key]["to_ts"] = max(grouped[key]["to_ts"], ts)

        return list(grouped.values())

    # Запись обработанных данных в БД
    async def saveData(self, dataIn):
        if not isinstance(dataIn, list) or len(dataIn) == 0:
            return False

        conn = None
        try:
            conn = await asyncpg.connect(
                user=self.db_config.get("user", "demo_user"),
                password=self.db_config.get("password", "demo_pw"),
                host=self.db_config.get("host", "localhost"),
                port=self.db_config.get("port", 5432),
                database=self.db_config.get("database", "demo_db"),
            )

            for row in dataIn:
                await conn.execute(
                    """
                    INSERT INTO demo_schema.demo_data(from_ts, to_ts, group_name, cnt)
                    VALUES (to_timestamp($1), to_timestamp($2), $3, $4)
                    """,
                    row["from_ts"],
                    row["to_ts"],
                    row["group_name"],
                    row["cnt"],
                )
            return True
        except Exception as e:
            logging.error(f"saveData error: {e}")
            return False
        finally:
            if conn:
                await conn.close()

    # Вызов выполнения итерации обработки: снятие -> обработка -> запись в БД
    #       srcConfig - объект с атрибутами подключения к Web API
    #       trgConfig - объект подключения к
    async def iteration(self, srcConfig, trgConfig=None):
        logging.info("Начало выполнения итерации")
        try:
            dataRaw = self.getData(srcConfig["url"])
            logging.info("Получено %d строк", len(dataRaw))

            dataPrepared = self.prepareData(
                dataRaw, srcConfig["from_ts"], srcConfig["to_ts"]
            )
            logging.info("Обработано %d групп", len(dataPrepared))

            success = await self.saveData(dataPrepared)
            if success:
                logging.info("Вставлено %d строк в БД", len(dataPrepared))
            else:
                logging.error("Ошибка при вставке данных в БД")

            logging.info("Итерация завершена. Успех: %s", success)
            return success
        except Exception as e:
            logging.error("Ошибка итерации: %s", e)
            return False
