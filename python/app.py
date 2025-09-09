import asyncio
import time
from libs.MainServer import MainServer
from tests.libs.EmulatorWebApi import EmulatorWebApi

async def start_iteration(src_config, trg_config):
    server = MainServer(trg_config)
    await server.iteration(src_config, trg_config)

async def run_app():
    web_apis = {
        "demo": {
            "url": "http://localhost:8040/api/read?from_ge=1754475000&to_lt=1755510000",
            "from_ts": 1754475000,
            "to_ts": 1755510000,
        },
        # можно добавить другие Web API
    }

    trg_config = {
        "host": "localhost",
        "port": 20432,
        "user": "demo_user",
        "password": "demo_pw",
        "database": "demo_db",
    }

    emulator_port = 8040
    emulator = EmulatorWebApi({"port": emulator_port})
    emulator_methods = {
        "get": {
            "/api/read": {
                "value": [
                    {"ts": 1754480000, "group_name": "группа 2", "cnt": 300},
                    {"ts": 1755490000, "group_name": "группа 1", "cnt": 400},
                    {"ts": 1755500000, "group_name": "группа 2", "cnt": 500},
                ],
                "fn": lambda rows, params: [
                    row for row in rows
                    if row["ts"] >= int(params.get("from_ge", 0)) 
                    and row["ts"] < int(params.get("to_lt", 1e10))
                ],
            }
        }
    }
    emulator.init(emulator_methods)
    emulator.start()
    print(f"Эмулятор запущен на порту {emulator_port}")
    time.sleep(0.1)  # дать серверу время подняться

    # Выбор Web API
    api_key = input(f"Выберите Web API ({'/'.join(web_apis.keys())}) [demo]: ") or "demo"
    src_config = web_apis.get(api_key, web_apis["demo"])
    print(f"Используется Web API: {api_key}")

    # Режим работы
    mode = input("Режим работы: 1 - однократная итерация, 2 - циклическая: ").strip() or "1"

    try:
        if mode == "2":
            interval_seconds = 10
            print(f"[INFO] Запуск циклической обработки каждые {interval_seconds} секунд")

            async def loop():
                while True:
                    try:
                        await start_iteration(src_config, trg_config)
                    except Exception as err:
                        print(f"[ERROR] Ошибка получения данных с {src_config['url']}: {err}")
                    await asyncio.sleep(interval_seconds)

            task = asyncio.create_task(loop())

            # Обработка Ctrl+C
            try:
                await task
            except KeyboardInterrupt:
                task.cancel()
                print("[INFO] Циклическая обработка остановлена")
        else:
            await start_iteration(src_config, trg_config)
    finally:
        emulator.stop()
        print("Эмулятор остановлен")

if __name__ == "__main__":
    asyncio.run(run_app())
