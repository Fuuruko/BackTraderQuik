from datetime import datetime, time
import backtrader as bt
from BackTraderQuik.QKStore import QKStore  # Хранилище QUIK


class Brackets(bt.Strategy):
    """
    Выставляем родительскую заявку на покупку на n% ниже цены закрытия
    Вместе с ней выставляем дочерние заявки на выход с n% убытком/прибылью
    При исполнении родительской заявки выставляем все дочерние
    При исполнении дочерней заявки отменяем все остальные неисполненные дочерние
    """
    params = (  # Параметры торговой системы
        ('LimitPct', 1),  # Заявка на покупку на n% ниже цены закрытия
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        dt = bt.num2date(self.datas[0].datetime[0]) if not dt else dt  # Заданная дата или дата текущего бара
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  # Выводим дату и время с заданным текстом на консоль

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные, затем перейдем в режим реальной торговли
        self.order = None  # Заявка на вход в позицию

    def next(self):
        """Получение следующего исторического/нового бара"""
        if not self.isLive:  # Если не в режиме реальной торговли
            return  # то выходим, дальше не продолжаем
        if self.order and self.order.status == bt.Order.Submitted:  # Если заявка не исполнена (отправлена брокеру)
            return  # то ждем исполнения, выходим, дальше не продолжаем
        if not self.position:  # Если позиции нет
            if self.order and self.order.status == bt.Order.Accepted:  # Если заявка не исполнена (принята брокером)
                self.cancel(self.order)  # то снимаем заявку на вход
            # Цена на n% ниже цены закрытия
            close_minus_n = self.data.close[0] * (1 - self.p.LimitPct / 100)  
            # Цена на 2n% ниже цены закрытия
            close_minus_2n = self.data.close[0] * (1 - self.p.LimitPct / 100 * 2)  
            # Родительская лимитная заявка на покупку
            # self.order = self.buy(exectype=bt.Order.Limit, price=close_minus_n, transmit=False)  
            # Дочерняя стоп заявка на продажу с убытком n%
            # orderStop = self.sell(exectype=bt.Order.Stop, price=close_minus_2n, size=self.order.size, parent=self.order, transmit=False)  
            # Дочерняя лимитная заявка на продажу с прибылью n%
            # orderLimit = self.sell(exectype=bt.Order.Limit, price=self.close[0], size=self.order.size, parent=self.order, transmit=True)  
            # Bracket заявка в BT
            self.order, orderStop, orderLimit = self.buy_bracket(limitprice=self.data.close[0], price=close_minus_n, stopprice=close_minus_2n)  

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статуса приходящих баров"""
        # Получаем статус (только при live=True)
        data_status = data._getstatusname(status)  
        # Не можем вывести в лог, т.к. первый статус DELAYED получаем до первого бара (и его даты)
        print(data_status)  
        # Режим реальной торговли
        self.isLive = data_status == 'LIVE'  

    def notify_order(self, order):
        """Изменение статуса заявки"""
        # Если заявка создана, отправлена брокеру, принята брокером (не исполнена)
        if order.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted):  
            self.log(f'Alive Status: {order.getstatusname()}. TransId={order.ref}')
        # Если заявка отменена, нет средств, заявка отклонена брокером, снята по времени (снята)
        elif order.status in (bt.Order.Canceled, bt.Order.Margin, bt.Order.Rejected, bt.Order.Expired):  
            self.log(f'Cancel Status: {order.getstatusname()}. TransId={order.ref}')
        # Если заявка частично исполнена
        elif order.status == bt.Order.Partial:  
            self.log(f'Part Status: {order.getstatusname()}. TransId={order.ref}')
        # Если заявка полностью исполнена
        elif order.status == bt.Order.Completed:  
            if order.isbuy():  # Заявка на покупку
                self.log(f'Bought @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            elif order.issell():  # Заявка на продажу
                self.log(f'Sold @{order.executed.price:.2f}, Cost={order.executed.value:.2f}, Comm={order.executed.comm:.2f}')
            self.order = None  # Сбрасываем заявку на вход в позицию

    def notify_trade(self, trade):
        """Изменение статуса позиции"""
        if trade.isclosed:  # Если позиция закрыта
            self.log(f'Trade Profit, Gross={trade.pnl:.2f}, NET={trade.pnlcomm:.2f}')


# Точка входа при запуске этого скрипта
if __name__ == '__main__':  
    # Инициируем "движок" BackTrader
    cerebro = bt.Cerebro()  

    # Код клиента (присваивается брокером)
    clientCode = '<Ваш код клиента>'  
    # Код фирмы (присваивается брокером)
    firmId = '<Код фирмы>'  
    # symbol = 'TQBR.SBER'  # Тикер
    # Для фьючерсов: <Код тикера>
    #                <Месяц экспирации: 3-H, 6-M, 9-U, 12-Z>
    #                <Последняя цифра года>
    symbol = 'SPBFUT.SiH3'  

    # Добавляем торговую систему с лимитным входом в n%
    cerebro.addstrategy(Brackets, LimitPct=1)  
    store = QKStore()  # Хранилище QUIK
    # Брокер со счетом фондового рынка РФ
    # broker = store.getbroker(use_positions=False, client_code=clientCode,
    #                          firm_id=firmId, trade_acc_id='L01-00000F00',
    #                          limit_kind=2, curr_code='SUR', is_futures=False)  
    # Брокер со счетом по умолчанию (срочный рынок РФ)
    broker = store.getbroker(use_positions=False)  
    # Устанавливаем брокера
    cerebro.setbroker(broker)  
    # Исторические и новые минутные бары за все время
    data = store.getdata(dataname=symbol, timeframe=bt.TimeFrame.Minutes, compression=1,
                         fromdate=datetime(2023, 2, 5), sessionstart=time(7, 0), live=True)  
    cerebro.adddata(data)  # Добавляем данные
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)  # Кол-во акций для покупки/продажи
    cerebro.run()  # Запуск торговой системы
