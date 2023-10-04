from datetime import date
from backtrader import Cerebro, TimeFrame
import Strategy as ts  # Торговые системы

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK

# Несколько тикеров для нескольких торговых систем по одному временнОму интервалу
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbols = ('TQBR.SBER', 'TQBR.GAZP', 'TQBR.LKOH', 'TQBR.GMKN',)  # Кортеж тикеров
    store = QKStore()  # Хранилище QUIK
    # Инициируем "движок" BackTrader.
    # Стандартная статистика сделок и кривой доходности не нужна
    cerebro = Cerebro(stdstats=False)
    for symbol in symbols:  # Пробегаемся по всем тикерам
        # Исторические и новые бары тикера с начала сессии
        data = store.getdata(dataname=symbol, live=True,
                             timeframe=TimeFrame.Minutes, compression=1,
                             fromdate=date.today())
        # Добавляем тикер
        cerebro.adddata(data)
    # Добавляем торговую систему по одному тикеру
    cerebro.addstrategy(ts.PrintStatusAndBars, name="One Ticker",
                        symbols=('TQBR.SBER',))
    # Добавляем торговую систему по двум тикерам
    cerebro.addstrategy(ts.PrintStatusAndBars, name="Two Tickers",
                        symbols=('TQBR.GAZP', 'TQBR.LKOH',))
    # Добавляем торговую систему по всем тикерам
    cerebro.addstrategy(ts.PrintStatusAndBars, name="All Tickers")
    cerebro.run()  # Запуск торговой системы
