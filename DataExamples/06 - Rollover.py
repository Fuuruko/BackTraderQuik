from datetime import time, datetime
from backtrader import Cerebro, feeds, TimeFrame
import Strategy as ts  # Торговые системы

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK

# Склейка тикера из файла и истории (Rollover)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер истории QUIK

    # Получаем историю из файла
    d1 = feeds.GenericCSVData(
        # Файл для импорта из QUIK. Создается из примера QuikPy Bars.py
        dataname=f'..\\..\\Data\\{symbol}_D1.txt',
        separator='\t',  # Колонки разделены табуляцией
        dtformat='%d.%m.%Y %H:%M',  # Формат даты/времени DD.MM.YYYY HH:MI
        openinterest=-1,  # Открытого интереса в файле нет
        # Для дневных данных и выше подставляется время окончания сессии.
        # Чтобы совпадало с историей, нужно поставить закрытие на 00:00
        sessionend=time(0, 0),
        fromdate=datetime(2020, 1, 1))  # Начальная дата и время приема исторических данных (Входит)

    store = QKStore()  # Хранилище QUIK
    d2 = store.getdata(dataname=symbol,
                       timeframe=TimeFrame.Days,
                       fromdate=datetime(2022, 12, 1))  # Получаем историю из QUIK

    # Инициируем "движок" BackTrader.
    # Стандартная статистика сделок и кривой доходности не нужна
    cerebro = Cerebro(stdstats=False)
    cerebro.rolloverdata(d1, d2, name=symbol)  # Склеенный тикер
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
