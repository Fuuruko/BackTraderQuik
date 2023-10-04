from backtrader import Cerebro, TimeFrame
import Strategy as ts  # Торговые системы

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK

# CAUSE AN ERROR. DON'T KNOW WHY
# Точное тестирование большего временного интервала с использованием меньшего (Replay)
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    store = QKStore()  # Хранилище QUIK
    # Инициируем "движок" BackTrader.
    # Стандартная статистика сделок и кривой доходности не нужна
    cerebro = Cerebro(stdstats=False)
    # Исторические данные по меньшему временному интервалу
    data = store.getdata(dataname=symbol,
                         timeframe=TimeFrame.Minutes, compression=5)
    # На графике видим большой интервал, прогоняем ТС на меньшем
    cerebro.replaydata(data, timeframe=TimeFrame.Days)
    # Добавляем торговую систему
    cerebro.addstrategy(ts.PrintStatusAndBars)
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
