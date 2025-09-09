# test_iteration_mode.py

import asyncio
import pytest

from libs.MainServer import MainServer
from tests.libs.EmulatorWebApi import EmulatorWebApi
from tests.libs.utils_postgres_asyncpg import exec_sql_requests, check_sql_results, create_connection

# Настройки для тестов
EMULATOR_PORT = 8050
SRC_CONFIG = {
    "url": f"http://localhost:{EMULATOR_PORT}/api/read?from_ge=1754475000&to_lt=1754500000",
    "from_ts": 1754475000,
    "to_ts": 1754500000,
}
TRG_CONFIG = {
    "host": "localhost",
    "port": 20433,
    "user": "demo_user",
    "password": "demo_pw",
    "database": "demo_db",
}


@pytest.fixture(scope="module")
def emulator():
    emu = EmulatorWebApi({"port": EMULATOR_PORT})
    emulator_methods = {
        "get": {
            "/api/read": {
                "value": [
                    {"ts": 1754480000, "group_name": "группа 2", "cnt": 300},
                    {"ts": 1754490000, "group_name": "группа 1", "cnt": 100},
                    {"ts": 1754485000, "group_name": "группа 1", "cnt": 200},
                ],
                "fn": lambda rows, params: [
                    row for row in rows
                        if row["ts"] >= int(params.get("from_ge", 0)) 
                        and row["ts"] < int(params.get("to_lt", 1e10))
                    ],
                }
        }
    }
    emu.init(emulator_methods)
    emu.start()
    yield emu
    emu.stop()


@pytest.fixture(autouse=True)
async def clean_db():
    # Подключение к базе и очистка таблицы перед каждым тестом
    conn = await create_connection(TRG_CONFIG)
    try:
        await exec_sql_requests(conn, [{"sql": "TRUNCATE TABLE demo_schema.demo_data"}])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_single_iteration_inserts_data(emulator):
    server = MainServer(TRG_CONFIG)
    result = await server.iteration(SRC_CONFIG, TRG_CONFIG)
    assert result is True

    conn = await create_connection(TRG_CONFIG)
    try:
        rows = await conn.fetch("SELECT * FROM demo_schema.demo_data")
        assert len(rows) == 2  # две группы
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cyclic_iteration_two_intervals(emulator):
    server = MainServer(TRG_CONFIG)

    async def cyclic_run(interval_seconds=1, iterations=2):
        for _ in range(iterations):
            await server.iteration(SRC_CONFIG, TRG_CONFIG)
            await asyncio.sleep(interval_seconds)

    await cyclic_run(interval_seconds=0.01, iterations=2)  # быстрый тест

    conn = await create_connection(TRG_CONFIG)
    try:
        rows = await conn.fetch("SELECT * FROM demo_schema.demo_data")
        assert len(rows) == 4  # две группы * 2 итерации
    finally:
        await conn.close()
