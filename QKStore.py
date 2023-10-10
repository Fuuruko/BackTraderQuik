from collections import deque
from datetime import datetime
from pytz import timezone

from backtrader.store import Store

# from backtrader import Order
# from backtrader.position import Position

from QuikPy import QuikPy
from .QKData import QKData
from .QKBroker import QKBroker


class QKStore(Store):
    """Хранилище QUIK"""

    params = (
        ('host', '127.0.0.1'),  # Адрес/IP компьютера с QUIK
    )

    # @classmethod
    def getdata(self, **kwargs):
        """Return QKData with args, kwargs"""
        data = self.DataCls(self, **kwargs)
        return data

    # @classmethod
    def getbroker(self, *args, **kwargs):
        """Return broker with *args, **kwargs from registered QKBroker"""
        broker = self.BrokerCls(self, **kwargs)
        return broker

    BrokerCls = QKBroker
    DataCls = QKData

    MarketTimeZone = timezone('Europe/Moscow')

    def __init__(self):
        super().__init__()
        self.notifs = deque()  # Уведомления хранилища
        # Вызываем конструктор QuikPy с адресом хоста
        self.provider = QuikPy(host=self.p.host)
        self.symbols = {}  # Информация о тикерах
        # Проверяем подключен ли QUIK к серверу брокера
        self.connected = self.provider.isConnected()
        # Список классов. В некоторых таблицах тикер указывается без кода класса
        self.class_codes = self.provider.getClassesList()
        self.subscribed_data = {}  # Словарь созданных дата классов

    def start(self):
        # Подключение терминала к серверу QUIK
        self.provider.OnConnected = self._on_connected
        # Отключение терминала от сервера QUIK
        self.provider.OnDisconnected = self._on_disconnected
        # Обработчик новых баров по подписке из QUIK
        self.provider.OnNewCandle = self._on_candle

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        """Выдача уведомлений хранилища"""
        self.notifs.append(None)
        return [notif for notif in iter(self.notifs.popleft, None)]

    def stop(self):
        # Возвращаем обработчик по умолчанию
        self.provider.OnNewCandle = self.provider.default_handler
        # Закрываем соединение для запросов и поток обработки функций обратного вызова
        self.provider.close_connection()

    # Функции

    def get_symbol_info(self, class_code, sec_code, reload=False):
        """Получение информации тикера

        :param str class_code: Код площадки
        :param str sec_code: Код тикера
        :param bool reload: Получить информацию из QUIK
        :return: Значение из кэша/QUIK или None, если тикер не найден
        """
        # Если нужно получить информацию из QUIK или
        # нет информации о тикере в справочнике
        if reload or (class_code, sec_code) not in self.symbols:
            # Получаем информацию о тикере из QUIK
            symbol_info = self.provider.getSecurityInfo(class_code, sec_code)
            if not symbol_info:
                print(f'Информация о {class_code}.{sec_code} не найдена')
                return None
            self.symbols[(class_code, sec_code)] = symbol_info
        return self.symbols[(class_code, sec_code)]

    def from_ticker(self, ticker):
        """Код площадки и код тикера из названия тикера(с кодом площадки или без него)

        str -  ticker - Название тикера
        return - Код площадки и код тикера
        """
        symbol_parts = ticker.split('.')
        # Если тикер задан в формате <Код площадки>.<Код тикера>
        if len(symbol_parts) >= 2:
            class_code = symbol_parts[0]
            sec_code = '.'.join(symbol_parts[1:])
        else:  # Если тикер задан без площадки
            # Получаем код площадки по коду инструмента из имеющихся классов
            class_code = self.provider.getSecurityClass(self.class_codes, ticker)
            sec_code = ticker  # Код тикера
        return class_code, sec_code

    @staticmethod
    def to_ticker(class_code: str, sec_code: str):
        """Название тикера из кода площадки и кода тикера

        class_code - Код площадки
        sec_code - Код тикера
        return - Название тикера
        """
        return f'{class_code}.{sec_code}'

    def _on_connected(self, data):
        """Обработка событий подключения к QUIK"""
        # Берем текущее время на бирже из локального
        dt = datetime.now(self.MarketTimeZone)
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}: QUIK Подключен')
        self.connected = True
        print(f'Проверка подписки тикеров ({len(self.subscribed_data)})')

        for sub_symb in self.subscribed_data.values():
            class_code = sub_symb.class_code  # Код площадки
            sec_code = sub_symb.sec_code  # Код тикера
            interval = sub_symb.interval  # Временной интервал
            print(f'{class_code}.{sec_code} на интервале {interval}', end=' ')

            # Если нет подписки на тикер/интервал
            if not self.provider.is_subs(class_code, sec_code, interval):
                self.provider.subs_to_candles(class_code, sec_code, interval)
                print('нет подписки. Отправлен запрос на новую подписку')
            else:  # Если подписка была, то переподписываться не нужно
                print('есть подписка')

    def _on_disconnected(self, data):
        """Обработка событий отключения от QUIK"""
        # Если QUIK отключен от сервера брокера,
        # то не нужно дублировать сообщение, выходим, дальше не продолжаем
        if not self.connected:
            return None
        dt = datetime.now(self.MarketTimeZone)
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}: QUIK Отключен')
        self.connected = False

    def _on_candle(self, data):
        dataname = f'{data["class"]}.{data["sec"]}_{data["interval"]}'
        self.subscribed_data[dataname].bars.append(data)
