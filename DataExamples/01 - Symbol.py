import Strategy as ts  # Торговые системы
from datetime import datetime, date, timedelta, time
from backtrader import Cerebro, TimeFrame

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK

# 6 примеров получения исторических и "живых" данных одного тикера по одному временнОму интервалу
if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    symbol = 'TQBR.SBER'  # Тикер
    # symbol = 'SPBFUT.SiH3'  # Для фьючерсов: <Код тикера>
    #                                          <Месяц экспирации: 3-H, 6-M, 9-U, 12-Z>
    #                                          <Последняя цифра года>

    store = QKStore()  # Хранилище QUIK (QUIK на локальном компьютере)
    # Хранилище QUIK (К QUIK на удаленном компьютере обращаемся по IP или названию)
    # store = QKStore(host='<Ваш IP адрес>')

    # Можно вызывать данные напрямую (не рекомендуется)
    # data = QKData(dataname=symbol, timeframe=TimeFrame.Days, Host='<Ваш IP адрес>')

    # Инициируем "движок" BackTrader. Стандартная статистика сделок и кривой доходности не нужна
    cerebro = Cerebro(stdstats=False)
    today = date.today()  # Сегодняшняя дата без времени
    week_ago = today - timedelta(days=7)  # Дата неделю назад без времени

    # 1. Все исторические дневные бары
    data = store.getdata(dataname=symbol, timeframe=TimeFrame.Days)

    # 2. Исторические часовые бары с дожи 4-х цен за текущий год
    # data = store.getdata(dataname=symbol, FourPriceDoji=True,
    #                      timeframe=TimeFrame.Minutes, compression=60,
    #                      fromdate=datetime(today.year, 1, 1),
    #                      todate=datetime(today.year, 12, 31),)

    # 3. Исторические 30-и минутные бары с заданной даты неделю назад до последнего бара
    # data = store.getdata(dataname=symbol,
    #                      timeframe=TimeFrame.Minutes, compression=30,
    #                      fromdate=week_ago)

    # 4. Исторические 5-и минутные бары первого часа текущей сессиИ без первой 5-и минутки
    # data = store.getdata(dataname=symbol,
    #                      timeframe=TimeFrame.Minutes, compression=5,
    #                      fromdate=datetime(today.year, today.month, today.day, 10, 5),
    #                      todate=datetime(today.year, today.month, today.day, 10, 55))

    # 5. Исторические 5-и минутные бары первого часа сессиЙ за неделю без первой 5-и минутки
    # data = store.getdata(dataname=symbol,
    #                      timeframe=TimeFrame.Minutes, compression=5,
    #                      fromdate=week_ago, todate=today,
    #                      sessionstart=time(10, 5), sessionend=time(11, 0))

    # 6. Исторические и новые минутные бары с начала сегодняшней сессии
    # data = store.getdata(dataname=symbol, live=True,
    #                      timeframe=TimeFrame.Minutes, compression=1,
    #                      fromdate=today)

    cerebro.adddata(data)  # Добавляем данные
    cerebro.addstrategy(ts.PrintStatusAndBars)  # Добавляем торговую систему
    cerebro.run()  # Запуск торговой системы
    cerebro.plot()  # Рисуем график
