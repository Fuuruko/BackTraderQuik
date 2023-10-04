import backtrader as bt


class PrintStatusAndBars(bt.Strategy):
    """
    - Отображает статус подключения
    - При приходе нового бара отображает его цены/объем
    - Отображает статус перехода к новым барам
    """
    params = (
        ('name', None),  # Название торговой системы
        ('symbols', None),  # Список торгуемых тикеров. По умолчанию торгуем все тикеры
    )

    def log(self, txt, dt=None):
        """Вывод строки с датой на консоль"""
        # Заданная дата или дата последнего бара первого тикера ТС
        dt = bt.num2date(self.datas[0].datetime[0]) if not dt else dt  
        # Выводим дату и время с заданным текстом на консоль
        print(f'{dt.strftime("%d.%m.%Y %H:%M")}, {txt}')  

    def __init__(self):
        """Инициализация торговой системы"""
        self.isLive = False  # Сначала будут приходить исторические данные

    def next(self):
        """Приход нового бара тикера"""
        if self.p.name:  # Если указали название торговой системы, то будем ждать прихода всех баров
            # Дата и время последнего бара каждого тикера
            lastdatetimes = [bt.num2date(data.datetime[0]) for data in self.datas]  
            # Если дата и время последних баров не идентичны
            if lastdatetimes.count(lastdatetimes[0]) != len(lastdatetimes):  
                return None # то еще не пришли все новые бары. Ждем дальше, выходим
            print(self.p.name)
        for data in self.datas:  # Пробегаемся по всем запрошенным тикерам
            # Если торгуем все тикеры или данный тикер
            if not self.p.symbols or data._name in self.p.symbols:  
                self.log(f'{data._name} - {bt.TimeFrame.Names[data.p.timeframe]} '
                         f'{data.p.compression} - Open={data.open[0]:.2f}, '
                         f'High={data.high[0]:.2f}, Low={data.low[0]:.2f}, '
                         f'Close={data.close[0]:.2f}, Volume={data.volume[0]:.0f}',
                         bt.num2date(data.datetime[0]))

    def notify_data(self, data, status, *args, **kwargs):
        """Изменение статсуса приходящих баров"""
        # Получаем статус (только при live=True)
        data_status = data._getstatusname(status)  
        # Статус приходит для каждого тикера отдельно
        print(f'{data._name} - {self.p.name} - {data_status}')  
        # В Live режим переходим после перехода первого тикера
        self.isLive = data_status == 'LIVE'  
