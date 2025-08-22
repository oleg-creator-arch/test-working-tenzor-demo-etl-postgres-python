# 
# эмулятор HTTP сервера - Web API
# 

# import json
# import logging
# import pathlib
# import urllib.parse

import os
import signal

from flask import Flask, jsonify, request
# from threading import Thread
from multiprocessing import Process, set_start_method
import time

class EmulatorWebApi:
    # Конструктор
    # config - объект конфга, ожидается целочисленный port - порт, на котором будет запущен сервер.
    def __init__(self, config):
        self.config = config
        self.server_thread = None

        self.app = Flask(__name__)
        self.port = config["port"] if "port" in config else 8082

        # Регистрация универсальных роутов для GET и POST
        self.app.route('/<path:subpath>', methods=['GET'])(self._handle_get)

    # Инициализация роуктов и ответов
    # endpoints - объект, включающий методы, внутри путри URL и для них данных для возврата 
    def init(self, endpoints):
        self.endpoints = endpoints

    # Обработчик для GET-запросов.
    def _handle_get(self, subpath):
        endpoint = f'/{subpath}'

        # return jsonify({'error': f"route: {endpoint}"}) #, 200

        if 'get' in self.endpoints and endpoint in self.endpoints['get']:
            endpoint_data = self.endpoints['get'][endpoint]
            # Проверяем, есть ли функция фильтрации
            if 'fn' in endpoint_data:
                fn = endpoint_data['fn']
                # Получаем параметры из строки запроса
                query_params = request.args
                # Вызываем функцию фильтрации с параметрами
                filtered_data = fn(endpoint_data['value'], query_params)
                return jsonify(filtered_data)
            else:
                return jsonify(endpoint_data['value'])
        else:
            return jsonify({'error': f"Endpoint not found: {endpoint}"}), 404

    # Запуск сервера.
    def start(self):
        # Лечение проблемы "Can't get local object 'Flask.__init__.<locals>.<lambda>'" на M1
        try:
            set_start_method('fork')
        except Exception as err: 
            None

        self.app_process = Process(
            target = self.app.run,
            kwargs = {
                "port": self.port
            },
        )

        self.app_process.start()

        # Ждем запуска сервера
        time.sleep(0.1)

    # Остановка сервера.
    def stop(self):
        self.app_process.terminate()
        self.app_process.join()