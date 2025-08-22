#  
#  Основной класс сервера приложений
#  Реализует обращение к источнику, обработку и запись в БД 
# 

import requests
import json

class MainServer:
    def __init__(self):
        # ничего не делаем
        None

    # Чтение данных из Web API источника
    def getData(self, srcUrl, timeout = 1000):
        # Заглушка - вместо константы необходимо реализовать получение данных через GET HTTP запрос по URL из srcUrl
        # return [
        #     { "ts": 1754480000, "group_name": "группа 2", "cnt": 300 },
        #     { "ts": 1755490000, "group_name": "группа 1", "cnt": 400 },
        #     { "ts": 1755500000, "group_name": "группа 2", "cnt": 500 },
        # ]

        # try:
            response = requests.get(srcUrl, timeout = timeout)
            response.raise_for_status()
            return response.json()
            

    # Обработка прочитанных данных
    #       dataIn - массив входящих строк 
    #       from_ts - метка времени начала (включая)
    #       to_ts - метка времени окончания периода (не включая)
    def prepareData(self, dataIn, from_ts, to_ts):
        #  Заглушка - пример трансформации корректно работающей на одной строке
        if(not dataIn):
            return []
        result = []
        for row in dataIn:
            result.append(
                {
                    "group_name": row["group_name"],
                    "cnt": row["cnt"],
                    "from_ts": from_ts,
                    "to_ts": to_ts
                }
            )
        return result

    # Запись обработанных данных в БД
    def saveData(self, dataIn):
        # Заглушка - без какой-либо эмуляции записи в БД 
        return True

    # Вызов выполнения итерации обработки: снятие -> обработка -> запись в БД 
    #       srcConfig - объект с атрибутами подключения к Web API
    #       trgConfig - объект подключения к 
    def iteration(srcConfig, trgConfig):
        # Заглушка - общий порядок действий 
        # Чтение
        dataRaw = this.getData(srcConfig)
        # Подготовка
        dataPrepared = this.prepareData(dataRaw, from_ts, to_ts)
        # Запись
        this.sendData(trgConfig, dataPrepared)