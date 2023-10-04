from datetime import date
from backtrader import Cerebro, TimeFrame
import Strategy as ts  # Торговые системы

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


# Получение данных одного тикера по разным временнЫм интервалам методом прямой загрузки из истории
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    store = QKStore()  # Хранилище QUIK
    # Инициируем "движок" BackTrader.
    # Стандартная статистика сделок и кривой доходности не нужна
    cerebro = Cerebro(stdstats=False)
    # Исторические данные по малому временнОму интервалу (должен идти первым)
    data = store.getdata(dataname=symbol,
                         timeframe=TimeFrame.Minutes, compression=1,
                         fromdate=date.today())
    cerebro.adddata(data)  # Добавляем данные
    # Исторические данные по большому временнОму интервалу
    data = store.getdata(dataname=symbol,
                         timeframe=TimeFrame.Minutes, compression=5,
                         fromdate=date.today())
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
