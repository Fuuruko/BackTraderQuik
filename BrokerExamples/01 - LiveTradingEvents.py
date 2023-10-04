from datetime import datetime, time
import backtrader as bt

# Для импортирования QKStore
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class LiveTradingEvents(bt.Strategy):
    """Получение и отображение событий в QUIK:

    - Изменение статуса приходящих баров (DELAYED / CONNECTED / DISCONNECTED / LIVE)
    - Получение нового бара
    - Изменение статуса заявок
    - Изменение статуса позиций
    Можно вручную открывать/закрывать позиции. В скрипте эта активность будет отображаться
    """

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        # Заданная дата или дата текущего бара
        dt = bt.num2date(self.datas[0].datetime[0]) if not dt else dt
        # Выводим дату и время с заданным текстом на консоль
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')

    def __init__(self):
        """Инициализация торговой системы"""
        # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.isLive = False

    def next(self):
        """Получение следующего исторического/нового бара"""
        for data in self.datas:  # Пробегаемся по всем запрошенным барам
            self.log(
                f'{data.p.dataname} Open={data.open[0]:.2f}, '
                f'High={data.high[0]:.2f}, Low={data.low[0]:.2f}, '
                f'Close={data.close[0]:.2f}, Volume={data.volume[0]:.0f}')
        if self.isLive:  # Если в режиме реальной торговли
            self.log(f'Свободные средства: {self.broker.getcash()}, Баланс: {self.broker.getvalue()}')

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        # Получаем статус (только при live=True)
        data_status = data._getstatusname(status)
        # Не можем вывести в лог, т.к. первый статус
        # DELAYED получаем до первого бара (и его даты)
        print(data_status)
        # Режим реальной торговли
        self.isLive = data_status == 'LIVE'

    def notify_order(self, order):
        """Изменение статуса заявки"""
        # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):
            self.log(f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
        elif order.status in (bt.Order.Canceled, bt.Order.Margin,
                              bt.Order.Rejected, bt.Order.Expired):
            self.log(f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Partial:  # Если заявка частично исполнена
            self.log(f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # Инициируем "движок" BackTrader.
    # Стандартная статистика сделок и кривой доходности не нужна
    cerebro = bt.Cerebro(stdstats=False)

    # Код клиента (присваивается брокером)
    clientCode = '<Ваш код клиента>'
    # Код фирмы (присваивается брокером)
    firmId = '<Код фирмы>'
    # symbol = 'TQBR.SBER'  # Тикер
    # Для фьючерсов: <Код тикера>
    #                <Месяц экспирации: 3-H, 6-M, 9-U, 12-Z>
    #                <Последняя цифра года>
    symbol = 'SPBFUT.SiH3'

    # Хранилище QUIK
    store = QKStore()

    # Брокер со счетом фондового рынка РФ
    # broker = store.getbroker(use_positions=False, client_code=clientCode,
    #                          firm_id=firmId, trade_acc_id='L01-00000F00',
    #                          limit_kind=2, curr_code='SUR', is_futures=False)
    # Брокер со счетом по умолчанию (срочный рынок РФ)
    broker = store.getbroker(use_positions=False)
    cerebro.setbroker(broker)  # Устанавливаем брокера

    # Исторические и новые минутные бары за все время
    data = store.getdata(dataname=symbol, live=True,
                         timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2023, 2, 5),
                         sessionstart=time(7, 0))

    cerebro.adddata(data)  # Добавляем данные

    # Добавляем торговую систему
    cerebro.addstrategy(LiveTradingEvents)
    # Кол-во акций для покупки/продажи
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    cerebro.run()  # Запуск торговой системы
