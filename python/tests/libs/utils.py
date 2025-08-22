# Вспомогательные функции работы с файлами для автотестов

import json
import decimal
from pathlib import Path
import requests


# Читает содержимое файла и возвращает его в виде строки.
def read_file(filename: str) -> str:
    with Path(filename).open(encoding="utf-8") as infile:
        return infile.read()

# Загружает содержимое файла с JSON в объект Python.
def load_json_file(filename: str) -> any:
    with Path(filename).open(encoding="utf-8") as f:
        return json.load(f)

# Класс для кодирования данных в JSON с обработкой Decimal 
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

# Сохраняет JSON в файл.
def save_json_to_file(filename: str, data: any, sort_keys: bool = False) -> None:
    Path(Path(filename).parent).mkdir(parents=True, exist_ok=True)
    with Path(filename).open(mode="w+", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False, sort_keys=sort_keys, cls=DecimalEncoder)

# Сохраняет текст в файл.
def save_to_file(filename: str, data: str) -> None:
    Path(Path(filename).parent).mkdir(parents=True, exist_ok=True)
    with Path(filename).open(mode="w+", encoding="utf-8") as outfile:
        outfile.write(data)

# Выполняет HTTP GET-запрос с указанным URL и возвратом ответа.
#   url: URL для выполнения GET-запроса.
#   timeout: Таймаут в секундах (по умолчанию 10 секунд).
def http_get_request(url, timeout=5):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при выполнении GET-запроса: {e}")
        return None


