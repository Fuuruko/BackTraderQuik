from datetime import datetime, timedelta, time
from collections import deque

from backtrader.feed import AbstractDataBase
from backtrader.metabase import MetaParams
from backtrader import TimeFrame, date2num
from backtrader.filters import SessionFilter


class QKData(AbstractDataBase):
    """Данные QUIK"""

    params = (
        # False - пропускать дожи 4-х цен, True - не пропускать
        ('FourPriceDoji', False),
        ('live', False),  # False - только история, True - история и новые бары
        ('count', 0)  # Кол-во полученных свечей. 0 - все доступные
    )

    _time_correction = timedelta(seconds=1)
    interval = 1

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload
        и runonce, т.к. новые бары должны идти один за другим
        """  # noqa: D205
        return self.p.live

    def __init__(self, store, **kwargs):
        # Для минутных временнЫх интервалов ставим кол-во минут
        match self.p.timeframe:
            case TimeFrame.Days:
                self.interval = 1440
            case TimeFrame.Weeks:
                self.interval = 10080
            case TimeFrame.Months:
                self.interval = 23200
        self.interval *= self.p.compression

        self.time_offset = timedelta(minutes=self.interval)

        # Передаем параметры в хранилище QUIK. Может работать самостоятельно, не через хранилище
        self.store = store
        self.prov = self.store.provider
        # По тикеру получаем код площадки и код тикера
        self.class_code, self.sec_code = self.store.from_ticker(self.p.dataname)

        self.bars = deque()

    def setenvironment(self, env):
        """Добавление хранилища QUIK в cerebro"""
        super().setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища QUIK в cerebro

    def start(self):
        super().start()
        if not self.p.FourPriceDoji:
            self.addfilter(DojiFilter)

        if (self.p.sessionstart != time.min
                or self.p.sessionend != time(23, 59, 59, 999990)):
            self.addfilter(SessionFilter)

        self.subs2bars()

    def subs2bars(self):
        # Отправляем уведомление об отправке исторических (не новых) баров
        self.put_notification(self.NOTSUBSCRIBED)
        # Добавляем в список подписанных тикеров/интервалов
        dataname = f'{self.class_code}.{self.sec_code}_{self.interval}'
        self.store.subscribed_data[dataname] = self

        self.bars.extend(self.prov.get_candles_ds(self.class_code, self.sec_code,
                                                  self.interval, self.p.count))

        if self.p.live:
            # Delete the last bar because it will return by subscription
            self.bars.pop()
            # TODO: Does is_subs check needed?
            if not self.prov.is_subs(self.class_code, self.sec_code, self.interval):
                self.prov.subs_to_candles(self.class_code, self.sec_code, self.interval)
        else:
            if self.is_unformed_bar(self.bars[-1]):
                self.bars.pop()

        if self.bars:
            # Отправляем уведомление о подключении и начале получения исторических баров
            self.put_notification(self.DELAYED)

    def _load(self):
        """Загружаем бар из истории или новый бар в BackTrader

        return None - Нового бара нет, но будет(в live)
        return False - Нового бара нет и не будет
        return True - Новый бар есть
        """
        if not self.bars:
            if not self.p.live:
                self.put_notification(self.DISCONNECTED)
                return False
            if self._laststatus != self.LIVE:
                self.put_notification(self.LIVE)
            return None

        bar = self.bars.popleft()
        # TODO: Could be removed
        if not self.is_old_bar(bar):
            return None

        # Бывает ситуация, когда QUIK несколько минут не передает новые бары,
        # а затем передает все пропущенные. Чтобы не совершать сделки на истории,
        # меняем режим торгов на историю до прихода нового бара
        # Если в LIVE режиме, и следующий бар не является LIVE
        if self._laststatus == self.LIVE and not bar['live']:
            # Отправляем уведомление об отправке исторических (не новых) баров
            self.put_notification(self.DELAYED)

        # Переводим в формат хранения даты/времени в BackTrader
        self.lines.datetime[0] = date2num(self.open_datetime(bar))
        # self.lines.open[0] = self.store.quik_to_bt_price(self.class_code, self.sec_code, bar['open'])
        # self.lines.high[0] = self.store.quik_to_bt_price(self.class_code, self.sec_code, bar['high'])
        # self.lines.low[0] = self.store.quik_to_bt_price(self.class_code, self.sec_code, bar['low'])
        # self.lines.close[0] = self.store.quik_to_bt_price(self.class_code, self.sec_code, bar['close'])
        self.lines.open[0] = bar['open']
        self.lines.high[0] = bar['high']
        self.lines.low[0] = bar['low']
        self.lines.close[0] = bar['close']
        self.lines.volume[0] = bar['volume']
        # Открытый интерес в QUIK не учитывается
        self.lines.openinterest[0] = 0
        return True

    def stop(self):
        super().stop()
        if self.p.live:
            self.prov.unsubs_from_candles(self.class_code, self.sec_code, self.interval)
        self.put_notification(self.DISCONNECTED)

    def haslivedata(self):
        return self._laststatus == self.LIVE

    # Функции

    def is_old_bar(self, bar):
        """Проверка бара на соответствие условиям выборки"""
        # Если получили несформированный бар. Например, дневной бар в середине сессии
        if self.is_unformed_bar(bar):
            print('ПРИШЕЛ НЕСФОРМИРОВАННЫЙ БАР ПО ПОДПИСКЕ\n'
                  'ОШИБКА. РАБОТАТЬ БУДЕТ, НО ТАК НЕ ДОЛЖНО БЫТЬ')
            return False
        # Если получили предыдущий или более старый бар
        # TODO: This as well may be removed since the check takes place in lua
        if date2num(self.open_datetime(bar)) <= self.lines.datetime[-1]:
            print('ПРИШЕЛ ПРЕДЫДУЩИЙ ИЛИ БОЛЕЕ СТАРЫЙ БАР ПО ПОДПИСКЕ.\n'
                  'ОШИБКА. РАБОТАТЬ БУДЕТ, НО ТАК НЕ ДОЛЖНО БЫТЬ')
            return False

        return True

    def is_unformed_bar(self, bar):
        # Дата/время закрытия бара
        dt_close = self.open_datetime(bar)
        dt_close += self.time_offset

        # Текущее биржевое время из QUIK. Корректируем его на несколько секунд,
        # т.к. минутный бар может прийти в 59 секунд прошлой минуты
        time_market_now = self.quik_datetime_now()  # Текущее биржевое время
        # time_market_now += self._time_correction

        # Если получили несформированный бар. Например, дневной бар в середине сессии
        if time_market_now < dt_close:
            return True
        return False

    @staticmethod
    def open_datetime(bar):
        """Дата/время открытия бара"""
        dt = bar['datetime']
        return datetime(dt['year'], dt['month'], dt['day'], dt['hour'], dt['min'])

    def quik_datetime_now(self):
        """Текущая дата и время

        - Если получили последний бар истории, то запрашием текущие дату и время из QUIK
        - Если находимся в режиме получения истории,
          то переводим текущие дату и время с компьютера в МСК
        """
        # Переводим строки в дату и время и возвращаем их
        if self.store.connected and self._laststatus == self.LIVE:
            # Может прийти неверная дата
            d = self.prov.getInfoParam('TRADEDATE')  # Дата dd.mm.yyyy
            t = self.prov.getInfoParam('SERVERTIME')  # Время hh:mi:ss
            return datetime.strptime(f'{d} {t}', '%d.%m.%Y %H:%M:%S')
        else:
            # Получаем МСК время из локального времени
            return datetime.now(self.store.MarketTimeZone).replace(tzinfo=None)


class DojiFilter(metaclass=MetaParams):
    def __init__(self, data):
        pass

    def __call__(self, data):
        '''Return Values:

        - False: data stream was not touched
        - True: data stream was manipulated (doji bar removed)
        '''  # noqa: D300
        if data.high[0] != data.low[0]:
            return False

        data.backwards()  # remove bar from data stack
        return True
