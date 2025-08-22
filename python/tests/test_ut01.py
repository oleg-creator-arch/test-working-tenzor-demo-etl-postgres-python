# Тесты 

import sys
import os

# Для тестирования
from tests.libs.utils import load_json_file, save_json_to_file, http_get_request
from tests.libs.utils_postgres_asyncpg import check_sql_results, exec_sql_requests, create_connection
from tests.libs.unittest_helper import TestCaseHelper
from tests.libs.EmulatorWebApi import EmulatorWebApi

import asyncio

# Тестируемый класс
from libs.MainServer import MainServer
# Корневой каталог проекта
ROOT_DIR = os.path.abspath(f"{os.path.dirname(os.path.abspath(__file__))}/../..")
# Корневой каталог ресурсов (файлов ицициализации / проверки) для тестов
TESTS_RESOURCES_DIR = os.path.abspath(f"{os.path.dirname(os.path.abspath(__file__))}/resources")

# Тесты полученя данных из WebAPI метода
class ut01_1_web_api(TestCaseHelper):    

    # Чтение 3х строк 
    def test_ut01_1_1__3_rows(self):
        test_id = 'ut01.1.1'

        # Тестовый каталог
        test_dir = self.make_test_dir(root_dir=ROOT_DIR, test_id=test_id)

        # Файлы - ожидаемый эталон и результат вызова тестируемого кода 
        expected_filename = f"{TESTS_RESOURCES_DIR}/ut01.1/{test_id}-expected.json"
        result_filename = f"{test_dir}/{test_id}-result.json"
        
        # ===  Тестовые данные 

        # Сервер - определяем эндпоинты эмулятора Web API, возвращаемые данные
        # Конкретно - задаём один ожидаемый эндпоинт get /api/read, который возвращает строки из массива value с наложением фильтра по ts

        emulator_port = 8082
        # Эмулятор источника - создание и запуск на заданном порту
        emulatorWebApi = EmulatorWebApi({"port": emulator_port})
        
        # Функция фильтрации для GET-запроса
        def filter_get_data(rows, params):
            filter_from = int(params["from_ge"]) if "from_ge" in params else 0
            filter_to   = int(params["to_lt"])   if "to_lt" in params else  1900000000

            rows_filtered = []
            for row in rows:
                if( row["ts"] >= filter_from and row["ts"] < filter_to):
                    rows_filtered.append(row)
            return rows_filtered
            
        emulatorMethods = {
            "get": {
                "/api/read": {
                    # Массив возвращаемых данных - на него накладывается фильтр в fn
                    "value": [
                        { "ts": 1754460000, "group_name": "группа 1", "cnt": 100 },
                        { "ts": 1754470000, "group_name": "группа 1", "cnt": 200 },
                        { "ts": 1754480000, "group_name": "группа 2", "cnt": 300 },
                        { "ts": 1755490000, "group_name": "группа 1", "cnt": 400 },
                        { "ts": 1755500000, "group_name": "группа 2", "cnt": 500 },
                    ],
                    # Функция для фильрации данных
                    "fn": filter_get_data
                }
            }
        }
        # Инициализация тестовых эндпоинтов и данных для ожидаемого ответа - с учётом функции фильттации 
        emulatorWebApi.init( emulatorMethods )
        emulatorWebApi.start()

        # === Объект тестирования

        app = MainServer()
        try:
            # Запрашиваем с наложением фильтра по TS
            resultData = app.getData(f"http://localhost:{emulator_port}/api/read?from_ge=1754475000&to_lt=1755510000")
        finally:
            emulatorWebApi.stop()

        # === Проверка результата

        # Полученное от HTTP запроса - в файл, с предварительной сортировкой JSON атрибутов
        save_json_to_file( result_filename, resultData, sort_keys=True)

        # Сравнение файлов
        self.compare_files(
            expected_result_filepath = expected_filename,
            actual_result_filepath   = result_filename
        )

# Тесты преобразования данных
class ut01_2_prepare(TestCaseHelper):
    
    # Чтение 3х строк 
    def test_ut01_2_1__1_row(self):
        test_id = 'ut01.2.1'

        # Тестовый каталог
        test_dir = self.make_test_dir(root_dir=ROOT_DIR, test_id=test_id)

        # Файлы - входящие данные, ожидаемый эталон и результат вызова тестируемого кода 
        data_filename = f"{TESTS_RESOURCES_DIR}/ut01.2/{test_id}-data-in.json"
        expected_filename = f"{TESTS_RESOURCES_DIR}/ut01.2/{test_id}-expected.json"
        result_filename = f"{test_dir}/{test_id}-result.json"
        
        # ===  Тестовые данные 

        # Входящие тестовые данные для обработки
        dataTestIn = load_json_file(data_filename)
        # 1. ответ от Web API - сами данные
        dataIn = dataTestIn["dataIn"]
        # 2. дата-время начала периода
        from_ts = dataTestIn["from_ts"]
        # 3. дата-время окончания периода
        to_ts = dataTestIn["to_ts"]

        # === Оъект тестирования

        app = MainServer()
        dataOut = app.prepareData(dataIn, from_ts, to_ts)

        # === Проверка результата

        # Полученное от HTTP запроса - в файл, с предварительной сортировкой JSON атрибутов
        save_json_to_file( result_filename, dataOut, sort_keys=True)

        # Сравнение файлов
        self.compare_files(
            expected_result_filepath = expected_filename,
            actual_result_filepath   = result_filename
        )

# Тесты записи в БД
class ut01_3_save(TestCaseHelper):

    # Ожидает что указанная в рекцизитах подключения СУБД работает и иницивлизироыванв 
    
    async def _coreTest(self, test_id):

        test_id = 'ut01.3.1'

        # Подключение к базе данных
        db_params = {
            'host': 'localhost',
            'port': '20433',
            'user': 'demo_user',
            'password': 'demo_pw',
            'database': 'demo_db',
        }

        # dbms = psycopg2.connect(**db_params)
        dbms = await create_connection(db_params)
        try:

            # Тестовый каталог
            test_dir = self.make_test_dir(root_dir=ROOT_DIR, test_id=test_id)

            # Файлы - входящие данные, ожидаемый эталон и результат вызова тестируемого кода 
            data_filename = f"{TESTS_RESOURCES_DIR}/ut01.3/{test_id}-data-in.json"
            expected_filename = f"{TESTS_RESOURCES_DIR}/ut01.3/{test_id}-expected.json"
            
            # ===  Тестовые данные 

            # Инициализировать данные в таблице БД - удалить и создать начальное состояние строк в таблице
            await exec_sql_requests( dbms, [
                { "sql": "truncate table demo_schema.demo_data" },
            ])

            # Входящие тестовые данные для обработки
            dataIn = load_json_file(data_filename)

            # === Оъект тестирования

            app = MainServer()
            writeResult = app.saveData(dataIn)

            # === Проверка результата

            # Проверяем что результат записи успешный
            assert(writeResult, "Результат записи в файл не положительный")
            
            # Прочитать строки из таблицы БД и сравнить с эталоном - на примере одной проверки
            await check_sql_results(dbms, test_dir, test_id, "general", [
                    {
                        "desc": "Данные из demo_db.demo_table",
                        "sql": "select extract( epoch from from_ts )::int8 as from_ts, extract( epoch from to_ts )::int8 as to_ts, group_name, cnt::bigint from demo_schema.demo_data order by group_name, from_ts, to_ts",
                        "expected_filename": expected_filename
                    }
                ],
                False,
                f"  in data filename: {data_filename}")
        finally:
            # Закрытие соединения
            await dbms.close()

    # Запись 1й строки 
    def test_ut01_3_1__1_row(self):
        asyncio.run(self._coreTest("ut01_3_1"))