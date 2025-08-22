# Класс для автотестов unittest с расширениями / обвязкой.

import copy
import shutil
import unittest
from pathlib import Path
import json

from tests.libs.utils import load_json_file, read_file
__unittest = True

# """Базовый класс, от которого должны наследоваться все модульные тесты."""
class TestCaseHelper(unittest.TestCase):

    # Сравнение содержимого двух файлов построчно.
    def compare_files(
        self,
        expected_result_filepath: str,
        actual_result_filepath: str,
        err_message: str = "",
    ) -> None:
        # Проверка что полные названия файлов разные
        self.assertNotEqual(
            expected_result_filepath,
            actual_result_filepath,
            f"{err_message}\nFilenames are equal {expected_result_filepath}",
        )
        # Проверка что файлы существуют
        self.assertTrue(
            Path(expected_result_filepath).is_file(),
            f"{err_message}\nFile with expected result is not found: "
            f"{expected_result_filepath}",
        )
        self.assertTrue(
            Path(actual_result_filepath).is_file(),
            f"{err_message}\nFile with actual result is not found: "
            f"{actual_result_filepath}",
        )

        # Чтение содержимого файлов
        expected_result = read_file(expected_result_filepath)
        actual_result = read_file(actual_result_filepath)

        self.assertEqual.__self__.maxDiff = None

        self.assertLinesEqual(
            expected_result,
            actual_result,
            "Compare files:\n  result: {}:{{line}}\n  expected: {}:{{line}}\n{}".format(
                actual_result_filepath, expected_result_filepath, err_message))

    # Сравнение содержимого двух JSON файлов (с предварительным форматированием) построчно.
    def compare_json_files(
        self,
        expected_result_filepath: str,
        actual_result_filepath: str,
        msg: str = "",
    ) -> None:
        """Сравнивает JSON-файлы."""
        expected_result = load_json_file(expected_result_filepath)
        actual_result = load_json_file(actual_result_filepath)

        # self.assertEqual(expected_result, actual_result)
        self.assertLinesEqual(
            json.dumps(actual_result, indent=0),
            json.dumps(expected_result, indent=0),
            "Compare files:\n  result: {}:{{line}}\n  expected: {}:{{line}}\n{}".format(actual_result_filepath, expected_result_filepath, msg))

    # Создает или очищает директорию для файлов, генерируемых тестом.
    @staticmethod
    def make_test_dir(root_dir: str, test_id: str) -> str:
        dir_test_tmp = f"{root_dir}/tmp/{test_id}"  # noqa: S108

        if Path(dir_test_tmp).is_dir():
            shutil.rmtree(dir_test_tmp)

        # Рекурсивное создание каталога
        Path(dir_test_tmp).mkdir(parents=True, exist_ok=True)
        return dir_test_tmp

    # Сравнение двух текстовых значений построчно
    def assertLinesEqual(self,
        cont1: str, cont2: str, comment: str = "") -> None:
        
        arr1 = cont1.split('\n')
        arr2 = cont2.split('\n')

        for index in range( 0, min( len(arr1), len(arr2)) ):
            self.assertEqual(arr1[index], arr2[index], "diff in line {}\n{}".format(
                    index+1, comment.format(line=(index+1)) or ""
                ))

        self.assertEqual(len(arr1), len(arr2), "{}\nlines count is diff, {} vs {}".format(
            len(arr1), len(arr2), comment.format(line=(min( len(arr1), len(arr2) )+1)) or ""))
    