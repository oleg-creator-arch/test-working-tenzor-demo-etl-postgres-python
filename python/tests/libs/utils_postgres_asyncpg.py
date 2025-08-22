#
# Методы тестов для подготовки / проверки данных в СУБД PostgreSQL
#

import os
import json
import asyncio
import asyncpg
from pathlib import Path
from tests.libs.unittest_helper import TestCaseHelper
from tests.libs.utils import save_json_to_file

# Создание подключения к БД - объекта dbm
async def create_connection(config):
    try:
        return await asyncpg.connect(
            user        = config["user"], 
            password    = config["password"],
            database    = config["database"], 
            host        = config["host"],
            port        = config["port"],
        )
    except Exception as error:
        raise ConnectionError(f"ERROR create_connection-001: database connaction failed\n{error}")

# Последовательное выполнение SQL-запросов.
async def exec_sql_requests(dbm, prepare_sql):
    if not dbm:
        raise ValueError("ERROR exec_sql_requests-001: database manager is not defined")

    if not prepare_sql:
        raise ValueError("WARNING exec_sql_requests-002: prepare_sql is not defined, stop exec_sql_requests")

    if not isinstance(prepare_sql, list):
        raise ValueError("ERROR exec_sql_requests-003: prepare_sql must be a list")

    if len(prepare_sql) == 0:
        raise ValueError("WARNING exec_sql_requests-004: prepare_sql is an empty list, stop exec_sql_requests")

    for step, item in enumerate(prepare_sql):
        try:
            if 'sql' in item:
                print(f"Prepare [{step}]: sql {item['sql']}")
                await dbm.execute(item['sql'])
            elif 'file' in item:
                file_path = os.path.abspath(item['file'])
                print(f"Prepare [{step}]: file {file_path}")
                with open(file_path, 'r') as f:
                    sql_script = f.read()
                await dbm.execute(sql_script)
            else:
                print(f"Prepare [{step}]: unknown type")
                raise ValueError(f"TEST wrong type of SQL: required 'sql' or 'file' attribute")
        except Exception as err:
            print(f"ERROR [{step}]: {err}")
            raise ValueError(f"TEST ERROR on SQL init item {item}:\n{err}")

# Проверка содержимого БД.
async def check_sql_results(dbm, dir_test_tmp, test_id, test_group, checks, flg_recreate_temp=True, message=""):
    if not checks or len(checks) == 0:
        raise ValueError("ERROR on check_sql_results: checks is empty")

    if not Path(dir_test_tmp).is_dir():
        raise ValueError(f"ERROR on check_sql_results: root dir does not exist, {dir_test_tmp}")

    if not test_group:
        test_group = ''

    for check_step, item in enumerate(checks):
        try:
            if not ( 'desc' in item ):
                item['desc'] = item['sql']

            print(f"Check [{check_step}]: {item['desc']}")
            result = await dbm.fetch(item['sql'])

            if not result:
                raise ValueError(f"[{check_step}] check '{item['desc']}': empty result")

            result_filename = os.path.join(dir_test_tmp, f"{test_id}-out-{test_group}-{check_step}.json")
            print(f"Files [{check_step}] '{item['desc']}':\n  result: {result_filename}\n  expected: {item['expected_filename']}")

            # Получаем результат запроса как JSON
            values = [dict(record) for record in result]
            # Сохраняем в файл как JSON
            save_json_to_file( result_filename, values, sort_keys=True)

            # Построчное сравнение файлов
            testHelper = TestCaseHelper()
            testHelper.compare_files(
                expected_result_filepath = item['expected_filename'],
                actual_result_filepath   = result_filename,
                err_message                  = message
            )

        except Exception as err:
            print(f"ERROR check [{check_step}]: {err}")
            raise ValueError(f"TEST ERROR on check [{check_step}]:\n{err}")
