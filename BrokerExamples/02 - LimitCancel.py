from datetime import datetime, time
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class LimitCancel(bt.Strategy):
    """
    Выставляем заявку на покупку на n% ниже цены закрытия
    Если за 1 бар заявка не срабатывает, то закрываем ее
    Если срабатывает, то закрываем позицию. Неважно, с прибылью или убытком
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(
            self.datas[0].datetime[0]) if not dt else dt  # Заданная дата или дата текущего бара
        # Выводим дату и время с заданным текстом на консоль
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход/выход из позиции

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        # Если заявка не исполнена (отправлена брокеру)
        if self.order and self.order.status == bt.Order.Submitted:
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            # Если заявка не исполнена (принята брокером)
            if self.order and self.order.status == bt.Order.Accepted:
                self.cancel(self.order)  # то снимаем ее
            # На n% ниже цены закрытия
            limit_price = self.data.close[0] * (1 - self.p.LimitPct / 100)
            # Лимитная заявка на покупку
            self.order = self.buy(exectype=bt.Order.Limit, price=limit_price)
        else:  # Если позиция есть
            self.order = self.close()  # Заявка на закрытие позиции по рыночной цене

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        data_status = data._getstatusname(
            status)  # Получаем статус (только при live=True)
        # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        print(data_status)
        self.isLive = data_status == 'LIVE'  # Режим реальной торговли

    def notify_order(self, order):
        """Изменение статуса заявки"""
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):  # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
            self.log(
                f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
        elif order.status in (bt.Order.Canceled, bt.Order.Margin, bt.Order.Rejected, bt.Order.Expired):
            self.log(
                f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Partial:  # Если заявка частично исполнена
            self.log(
                f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        elif order.status == bt.Order.Completed:  # Если заявка полностью исполнена
            if order.isbuy():  # Заявка на покупку
                self.log(
                    f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(
                    f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(
                f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    cerebro = bt.Cerebro()  # Инициируем "движок" BackTrader

    clientCode = '<Ваш код клиента>'  # Код клиента (присваивается брокером)
    firmId = '<Код фирмы>'  # Код фирмы (присваивается брокером)
    # symbol = 'TQBR.SBER'  # Тикер
    # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z><Последняя цифра года>
    symbol = 'SPBFUT.SiH3'

    # Добавляем торговую систему с лимитным входом в n%
    cerebro.addstrategy(LimitCancel, LimitPct=1)
    store = QKStore()  # Хранилище QUIK
    # broker = store.getbroker(use_positions=False, client_code=clientCode, firm_id=firmId, trade_acc_id='L01-00000F00', limit_kind=2, curr_code='SUR', is_futures=False)  # Брокер со счетом фондового рынка РФ
    # Брокер со счетом по умолчанию (срочный рынок РФ)
    broker = store.getbroker(use_positions=False)
    cerebro.setbroker(broker)  # Устанавливаем брокера
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2023, 2, 5), sessionstart=time(7, 0), live=True)  # Исторические и новые минутные бары за все время
    cerebro.adddata(data)  # Добавляем данные
    # Кол-во акций для покупки/продажи
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    cerebro.run()  # Запуск торговой системы
