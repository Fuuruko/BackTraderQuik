import time
from collections import deque, defaultdict, OrderedDict
from datetime import date, datetime

from backtrader import MetaBroker, BrokerBase, Order
from backtrader.position import Position


class QKBroker(BrokerBase, metaclass=MetaBroker):
    """Брокер QUIK"""

    # TODO Сделать обертку для поддержки множества брокеров
    # TODO Сделать пример постановки заявок по разным портфелям
    # Обсуждение решения: https://community.backtrader.com/topic/1165/does-backtrader-support-multiple-brokers
    # Пример решения: https://github.com/JacobHanouna/backtrader/blob/ccxt_multi_broker/backtrader/brokers/ccxtmultibroker.py

    params = (
        # При запуске брокера подтягиваются текущие позиции с биржи
        ('use_positions', True),
        ('is_lots', True),  # Входящий остаток в лотах (задается брокером)
        ('client_code', ''),  # Код клиента
        # По статье https://zen.yandex.ru/media/id/5e9a612424270736479fad54/bitva-s-finam-624f12acc3c38f063178ca95
        # Номер торгового терминала.
        # У брокера Финам требуется для совершения торговых операций
        ('order_client_code', ''),
        ('firm_id', 'SPBFUT'),  # Фирма
        ('trade_acc_id', 'SPBFUT00PST'),  # Счет
        ('limit_kind', 0),  # Вид лимита
        ('curr_code', 'SUR'),  # Валюта
        ('is_futures', True),  # Фьючерсный счет
        # Размер в минимальных шагах цены инструмента для исполнения стоп заявок
        ('stop_steps', 10),
    )

    def __init__(self, store, **kwargs):
        super().__init__()
        self.store = store
        self.prov = self.store.provider

        self.notifs = deque()  # Очередь уведомлений брокера о заявках

        # Стартовые и текущие свободные средства по счету
        self.startingcash = self.cash = 0
        # Стартовый и текущий баланс счета
        self.startingvalue = self.value = 0

        # Для брокера Финам нужно вместо кода клиента
        # указать номер торгового терминала
        if not self.p.order_client_code:
            self.p.order_client_code = self.p.client_code

        # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.trade_nums = defaultdict(list)
        self.positions = defaultdict(Position)  # Список позиций
        # Список заявок, отправленных на биржу
        self.orders = OrderedDict()

        # Список связанных заявок (One Cancel Others)
        self.ocos = {}
        # TODO: Replace deque to list maybe?
        # Очередь всех родительских/дочерних заявок (Parent - Children)
        self.pcs = defaultdict(deque)

    def start(self):
        super().start()
        # Ответ на транзакцию пользователя
        self.prov.OnTransReply = self.on_trans_reply
        # Получение новой / изменение существующей сделки
        self.prov.OnTrade = self.on_trade

        # Если нужно при запуске брокера получить текущие позиции на бирже
        if self.p.use_positions:
            """Все активные позиции по счету"""
            if self.p.is_futures:
                self.get_futures_positions()
            else:
                self.get_other_positions()

        # Стартовые и текущие свободные средства по счету
        self.startingcash = self.cash = self.getcash()
        # Стартовый и текущий баланс счета
        self.startingvalue = self.value = self.getvalue()

    def stop(self):
        super().stop()
        (
            self.prov.OnConnected,     # Соединение терминала с сервером QUIK
            self.prov.OnDisconnected,  # Отключение терминала от сервера QUIK
            self.prov.OnTransReply,    # Ответ на транзакцию пользователя
            self.prov.OnTrade,         # Получение новой / изменение существующей сделки
        ) = (self.prov.default_handler,) * 4

    # Get functions

    def getcash(self):
        """Свободные средства по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        if self.p.is_futures:
            cash = self.get_futures_limits()
        else:
            cash = self.get_other_limits()

        if cash:
            self.cash = cash
        return self.cash

    def getvalue(self, datas=None):
        """Стоимость позиций по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        # TODO Выдавать баланс по тикерам (datas) как в Alor
        # TODO Выдавать весь баланс, если не указаны параметры.
        #      Иначе, выдавать баланс по параметрам
        value = self.get_positions_limits()
        if value:
            # Баланс счета = свободные средства + стоимость позиций
            self.value = value
        return self.value

    def getposition(self, data):
        """Позиция по тикеру

        Используется в strategy.py для закрытия (close)
        и ребалансировки (увеличения/уменьшения) позиции:
        - В процентах от портфеля (order_target_percent)
        - До нужного кол-ва (order_target_size)
        - До нужного объема (order_target_value)
        """
        # Получаем позицию по тикеру или нулевую позицию,
        # если тикера в списке позиций нет
        class_sec_code = self.store.from_ticker(data._name)
        return self.positions[class_sec_code]

    # Get subfunctions

    def get_futures_positions(self):
        class_code = 'SPBFUT'
        # Все фьючерсные позиции
        futur_holds = self.prov.getFuturesHolding()
        # Активные фьючерсные позиции
        active_futur_holds = [f_h for f_h in futur_holds if not f_h['totalnet']]
        for act_futur_hold in active_futur_holds:
            class_sec_code = class_code, act_futur_hold['sec_code']

            lots = act_futur_hold['totalnet']  # Кол-во открытых позиций
            lots = self.size_to_lots(*class_sec_code, lots)

            price = act_futur_hold['avrposnprice']  # Цена позиций
            # Сохраняем в списке открытых позиций
            self.positions[class_sec_code] = Position(lots, price)

    def get_other_positions(self):
        # Для остальных фирм
        # Все лимиты по бумагам (позиции по инструментам)
        depo_limits = self.prov.get_depo_limits()
        acc_depo_limits = [depo_limit for depo_limit in depo_limits
                           if depo_limit['client_code'] == self.p.client_code
                           and depo_limit['firmid'] == self.p.firm_id
                           and depo_limit['limit_kind'] == self.p.limit_kind  # вид лимита
                           and depo_limit['currentbal'] != 0]  # только открытые позиции
        for depo_limit in acc_depo_limits:
            # По коду тикера без площадки получаем код площадки и код тикера
            # TODO: Another class code with the same ticker may come
            class_sec_code = self.store.from_ticker(depo_limit['sec_code'])
            lots = depo_limit['currentbal']  # Кол-во открытых позиций
            lots = self.size_to_lots(*class_sec_code, lots)
            # Средневзвешенная цена позиций
            price = depo_limit['wa_position_price']
            # Сохраняем в списке открытых позиций
            self.positions[class_sec_code] = Position(lots, price)

    def get_futures_limits(self):
        """
        Для фьючерсов свои расчеты
        Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
        Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
        Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
        Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комиссии.
            Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
        Накоплен.доход включает Биржевые сборы
        Тек.чист.поз. = Заблокированное ГО под открытые позиции
        План.чист.поз. = На какую сумму можете открыть еще позиции
        """
        futures_limit = self.prov.getFuturesLimit(self.p.firm_id,
                                                  self.p.trade_acc_id,
                                                  0, 'SUR')
        # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
        if futures_limit:
            return (futures_limit['cbplimit']
                    + futures_limit['varmargin']
                    + futures_limit['accruedint'])
        else:
            print(f'QUIK не вернул фьючерсные лимиты: '
                  f'firm_id={self.p.firm_id}, trade_acc_id={self.p.trade_acc_id}. '
                  f'Проверьте правильность значений')
            return None

    def get_other_limits(self):
        # Для остальных фирм
        # Все денежные лимиты (остатки на счетах)
        money_limits = self.prov.getMoneyLimits()
        if not money_limits:
            print('QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return None

        cash = [money_limit for money_limit in money_limits
                if money_limit['client_code'] == self.p.client_code
                and money_limit['firmid'] == self.p.firm_id
                and money_limit['limit_kind'] == self.p.limit_kind  # Вид лимита
                and money_limit["currcode"] == self.p.curr_code]  # Код валюты
        # NOTE: Could cash be more than 1?
        if not cash:  # Если ни один денежный лимит не подходит
            print(f'Денежный лимит не найден:\n\t'
                  f'client_code={self.p.client_code}, firm_id={self.p.firm_id}, '
                  f'limit_kind={self.p.limit_kind}, curr_code={self.p.curr_code}'
                  f'\nПроверьте правильность значений')
            # Для отладки, если нужно разобраться, что указано неверно
            # print(f'Полученные денежные лимиты: {money_limits}')
            return None
        else:
            # Денежный лимит (остаток) по счету
            return cash[0]['currentbal']

    def get_positions_limits(self):
        """Стоимость позиций по счету"""
        if self.p.is_futures:  # Для фьючерсов свои расчеты
            futures_limit = self.prov.getFuturesLimit(self.p.firm_id,
                                                      self.p.trade_acc_id,
                                                      0, 'SUR')
            # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
            return futures_limit['cbplused'] if futures_limit else None

        # Для остальных фирм
        pos_value = 0  # Стоимость позиций по счету
        for class_sec_code, pos in self.positions.items():
            # Последняя цена сделки
            last_price = self.prov.getParamEx(*class_sec_code, 'LAST')['param_value']
            pos_value += pos.size * last_price  # Добавляем стоимость позиции
        return pos_value

    # Notifications

    def get_notification(self):
        if not self.notifs:
            return None
        return self.notifs.popleft()

    def next(self):
        self.notifs.append(None)  # mark notificatino boundary

    # Orders fucntions

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, oco=None, trailamount=None,
            trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit,
                                  exectype, Order.Buy, valid, oco,
                                  parent, transmit, **kwargs)
        # Уведомляем брокера об отправке новой заявки на покупку на биржу
        self.notifs.append(order.clone())
        return order

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, oco=None, trailamount=None,
             trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit,
                                  exectype, Order.Sell, valid, oco,
                                  parent, transmit, **kwargs)
        # Уведомляем брокера об отправке новой заявки на продажу на биржу
        self.notifs.append(order.clone())
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def create_order(self, owner, data, size, price=None, plimit=None,
                     exectype=None, ordtype=Order.Buy, valid=None, oco=None,
                     parent=None, transmit=True,  **kwargs):
        """Создание заявки.

        Привязка параметров счета и тикера.
        Обработка связанных и родительской/дочерних заявок
        """
        order = Order(owner=owner, data=data, size=size, price=price,
                      pricelimit=plimit, exectype=exectype, ordtype=ordtype,
                      valid=valid, oco=oco, parent=parent, transmit=transmit)

        class_sec_code = self.store.from_ticker(data._name)

        self.update_order_info(order, data, class_sec_code, kwargs)

        if oco:  # Если есть связанная заявка
            # то заносим в список связанных заявок
            self.ocos[order.ref] = oco.ref

        if not transmit or parent:  # Для родительской/дочерних заявок
            # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            parent_ref = getattr(order.parent, 'ref', order.ref)
            # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
            if order.ref != parent_ref and parent_ref not in self.pcs:
                print(f'Постановка заявки {order.ref} '
                      f'по тикеру {".".join(class_sec_code)} отменена. '
                      f'Родительская заявка не найдена')
                order.reject(self)
                return order

            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)

        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            # Уведомляем брокера о создании новой заявки
            self.notifs.append(order.clone())
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                # Отправляем родительскую заявку на биржу
                return self.place_order(order.parent)
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False),
        # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим
        return order

    def place_order(self, order):
        """Отправка заявки (транзакции) на биржу"""
        # Все значения должны передаваться в виде строк
        transaction = {
            'TRANS_ID': str(order.ref),                  # Номер транзакции задается клиентом
            'CLIENT_CODE': order.info['client_code'],    # Код клиента. Для фьючерсов его нет
            'ACCOUNT': order.info['trade_acc_id'],       # Счет
            'CLASSCODE': order.info['class_code'],       # Код площадки
            'SECCODE': order.info['sec_code'],           # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'QUANTITY': str(abs(order.size)),            # Кол-во в лотах
            }

        match order.exectype:
            case Order.Market:
                self.if_market(order, transaction)
            case Order.Limit:
                self.if_limit(order, transaction)
            case Order.Stop:
                self.if_stop(order, transaction)

        response = self.prov.sendTransaction(transaction)  # Отправляем транзакцию на биржу

        if self.is_error(response):
            order.reject(self)  # Отклонена
        else:
            order.submit(self)  # Принята

        # Сохраняем в списке заявок, отправленных на биржу
        self.orders[order.ref] = order
        return order

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return None
        if order.ref not in self.orders:  # Если заявка не найдена
            return None

        order_num = order.info['order_num']
        # Задана стоп заявка и лимитная заявка не выставлена
        is_stop = (order.exectype in [Order.Stop, Order.StopLimit]
                   and self.prov.get_order_by_num(order_num))
        # NOTE: Client code and Account don't needed?
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': order.info['class_code'],
            'SECCODE': order.info['sec_code'],
            }

        if is_stop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'
            transaction['STOP_ORDER_KEY'] = str(order_num)
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'
            transaction['ORDER_KEY'] = str(order_num)
        self.prov.sendTransaction(transaction)
        # В список уведомлений ничего не добавляем. Ждем события OnTransReply
        return order

    # Quik callbacks

    def on_trans_reply(self, trans_reply):
        """Обработчик события ответа на транзакцию пользователя"""
        trans_id = trans_reply['trans_id']
        order_num = trans_reply['order_num']  # Номер заявки на бирже

        if not self.check_trans_id(trans_id, order_num):
            return None

        order = self.orders[trans_id]  # Ищем заявку по номеру транзакции
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже

        result_msg = trans_reply['result_msg'].lower()
        status = trans_reply['status']
        # TODO: status == 3 mean accepted as well
        # TODO: analyze by status
        # NOTE: This error may occur because broker start before data come
        #       and self.data.datetime used in (margin(),cancel(),reject(self)) is empty
        try:
            # Если пришел ответ по новой заявке
            print('trans status: ', status, end='\t')
            if status == 15 or 'зарегистрирован' in result_msg:  
                order.accept(self)  # Заявка принята на бирже (Order.Accepted)
            elif 'снят' in result_msg:  # Если пришел ответ по отмене существующей заявки
                order.cancel()  # Отменяем существующую заявку (Order.Canceled)
            # Транзакция не выполнена (ошибка заявки):
            elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):
                # - Не найдена заявка для удаления
                # - Вы не можете снять данную заявку
                # - Превышен лимит отправки транзакций для данного логина
                # TODO: Nothing to do with order?
                if (status == 4 and 'не найдена заявка' in result_msg
                        or status == 5 and 'не можете снять' in result_msg
                        or 'превышен лимит' in result_msg):
                    return None
                order.reject(self)  # Отклоняем заявку (Order.Rejected)
            elif status == 6:
                order.margin()  # Для заявки не хватает средств (Order.Margin)
            print('order status: ', order.getstatusname())
        except (KeyError, IndexError):
            pass
        self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:
            # Проверяем связанные и родительскую/дочерние заявки (Canceled, Rejected, Margin)
            self.oco_check(order)
            self.pc_check(order)

    def on_trade(self, trade):
        """Обработчик события получения новой / изменения существующей сделки.

        Выполняется до события изменения существующей заявки.
        Нужен для определения цены исполнения заявок.
        """
        order_num = trade['order_num']
        class_sec_code = trade['class_code'], trade['sec_code']
        # По номеру заявки в сделке пробуем получить заявку с биржи
        quik_order = self.prov.get_order_by_num(order_num)

        if not self.order_on_exchange(quik_order, order_num):
            return None
        if not self.check_trans_id(quik_order['trans_id'], order_num):
            return None
        if not self.check_duplication(trade['trade_num'], class_sec_code):
            return None

        # Ищем заявку по номеру транзакции
        order = self.orders[quik_order['trans_id']]

        self.update_order(trade, order, class_sec_code)

        # Уведомляем брокера об исполнении заявки
        self.notifs.append(order.clone())

        # Если заявка исполнена полностью (ничего нет к исполнению)
        if order.status == order.Completed:
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            # Проверяем связанные и родительскую/дочерние заявки (Completed)
            self.oco_check(order)
            self.pc_check(order)

    # Check functions

    def check_duplication(self, trade_num, class_sec_code):
        """Проверка на дублирование транзакции"""
        # Если номер сделки есть в списке (фильтр для дублей)
        if trade_num in self.trade_nums[class_sec_code]:
            return False
        # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        self.trade_nums[class_sec_code].append(trade_num)
        return True

    def check_trans_id(self, trans_id, order_num):
        # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
        if trans_id == 0:
            return False
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            print(f'Заявка с номером {order_num} и номером транзакции '
                  f'{trans_id} была выставлена не из торговой системы')
            return False
        return True

    def order_on_exchange(self, order, order_num):
        # Если заявка не найдена, значит order=False
        # Возможно заявка есть, но она не успела прийти к брокеру
        if not order:
            print(f'Заявка с номером {order_num} не найдена на бирже с 1-ой попытки. '
                  f'Через 3с будет 2-ая попытка')
            time.sleep(3)
            # Снова пробуем получить заявку с биржи по ее номеру
            order = self.prov.get_order_by_num(order_num)
            if not order:
                print(f'Заявка с номером {order_num} не найдена на бирже со 2-ой попытки')
                return False
        return True

    def oco_check(self, order):
        """Проверка связанных заявок"""
        for order_ref, oco_ref in self.ocos.items():
            # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
            if oco_ref == order.ref:
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            # то получаем номер транзакции связанной заявки
            oco_ref = self.ocos[order.ref]
            # отменяем связанную заявку
            self.cancel_order(self.orders[oco_ref])

    def pc_check(self, order):
        """Проверка родительской/дочерних заявок"""
        # Если исполнена родительская заявка
        if (not order.parent and not order.transmit
                and order.status == Order.Completed):
            # Получаем очередь родительской/дочерних заявок
            pcs = self.pcs[order.ref]
            for child in pcs:  # Пробегаемся по всем заявкам
                if child.parent:  # Пропускаем первую (родительскую) заявку
                    # Отправляем дочернюю заявку на биржу
                    self.place_order(child)
        elif order.parent:  # Если исполнена/отменена дочерняя заявка
            # Получаем очередь родительской/дочерних заявок
            pcs = self.pcs[order.parent.ref]
            for child in pcs:  # Пробегаемся по всем заявкам
                # Пропускаем первую (родительскую) заявку и исполненную заявку
                if child.parent and child.ref != order.ref:
                    self.cancel_order(child)  # Отменяем дочернюю заявку

    def is_error(self, response):
        # Если возникла ошибка при постановке заявки на уровне QUIK
        if 'lua_error' in response:
            # то заявка не отправляется на биржу, выводим сообщение об ошибке
            print(f'Ошибка отправки заявки в QUIK '
                  f'{response["CLASSCODE"]}.{response["SECCODE"]} '
                  f'{response["lua_error"]}')
            return True
        return False

    # Other functions

    def update_order(self, trade, order, class_sec_code):
        # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        order.addinfo(order_num=trade['order_num'])

        lots = trade['qty']  # Абсолютное кол-во
        lots = self.size_to_lots(*class_sec_code, lots)
        # Если сделка на продажу (бит 2)
        if trade['flags'] & 0b100 == 0b100:
            lots *= -1  # то кол-во ставим отрицательным

        price = trade['price']
        # NOTE: Error may occur because broker starts before data arrives,
        #       so order.data.datetime is empty
        try:
            # Дата и время исполнения заявки. Последняя известная
            dt = order.data.datetime[0]
        except (KeyError, IndexError):
            # Берем текущее время на бирже из локального
            dt = datetime.now(self.store.MarketTimeZone)

        # Получаем позицию по тикеру или нулевую позицию если тикера в списке позиций нет
        pos = self.getposition(order.data)
        # Обновляем размер/цену позиции на размер/цену сделки
        psize, pprice, opened, closed = pos.update(lots, price)

        # Исполняем заявку в BackTrader
        order.execute(dt=dt, size=lots, price=price,
                      closed=closed, closedvalue=0, closedcomm=0,
                      opened=opened, openedvalue=0, opencomm=0,
                      margin=0, pnl=0,
                      psize=psize, pprice=pprice)

    def update_order_info(self, order, data, class_sec_code, kwargs):
        # Получаем параметры тикера (min_price_step, scale)
        si = self.store.get_symbol_info(*class_sec_code)
        if not si:
            print(f'Постановка заявки {order.ref} по тикеру '
                  f'{".".join(class_sec_code)} отменена. Тикер не найден')
            order.reject(self)
            return order
        # По тикеру выставляем комиссии в заявку.
        # Нужно для исполнения заявки в BackTrader
        order.addcomminfo(self.getcommissioninfo(data))

        order.addinfo(client_code=self.p.order_client_code,
                      trade_acc_id=self.p.trade_acc_id)
        order.addinfo(class_code=class_sec_code[0], sec_code=class_sec_code[1])

        order.addinfo(min_price_step=si['min_price_step'])
        # Размер проскальзывания в деньгах
        order.addinfo(slippage=si['min_price_step'] * self.p.stop_steps)
        # Кол-во значащих цифр после запятой
        order.addinfo(scale=si['scale'])
        # Передаем в заявку все дополнительные свойства из брокера
        order.addinfo(**kwargs)

    def size_to_lots(self, class_code, sec_code, size: int):
        """Перевод кол-ва из штук в лоты"""
        # Получаем параметры тикера (lot_size)
        if not self.p.is_lots:
            si = self.store.get_symbol_info(class_code, sec_code)
            if not si:  # Если тикер не найден
                return size
            lot_size = si['lot_size']
            return size // lot_size if lot_size > 0 else size
        return size

    # Transaction functions

    def if_market(self, order, trans):
        """Market order"""
        class_sec_code = order.info['class_code'], order.info['sec_code']
        slippage = order.info['slippage']  # Размер проскальзывания в деньгах
        scale = order.info['scale']

        if class_sec_code[0] == 'SPBFUT':  # Для рынка фьючерсов
            # Последняя цена сделки
            price = self.prov.getParamEx(*class_sec_code, 'LAST')['param_value']
            # Из документации QUIK: При покупке/продаже фьючерсов
            # по рынку нужно ставить цену хуже последней сделки
            price = price + (slippage if self.isbuy() else -slippage)
        else:
            # Цена рыночной заявки должна быть нулевой (кроме фьючерсов)
            price = 0

        trans['ACTION'] = 'NEW_ORDER'
        trans['TYPE'] = 'M'
        trans['PRICE'] = f'{price:.{scale}f}'

    def if_limit(self, order, trans):
        """Limit order"""
        price = order.price
        scale = order.info['scale']

        trans['ACTION'] = 'NEW_ORDER'
        trans['TYPE'] = 'L'
        trans['PRICE'] = f'{price:.{scale}}'

    # TODO: idk what happend here
    def if_stop(self, order, trans):
        """Stop order"""
        price = order.price
        plimit = order.plimit  # Лимитная цена исполнения
        slippage = order.info['slippage']  # Размер проскальзывания в деньгах
        scale = order.info['scale']

        trans['PRICE'] = f'{price:.{scale}}'

        if not plimit:
            # Покупаем/продаем по большей/меньшей цене в размер проскальзывания
            plimit = price + (slippage if order.isbuy() else -slippage)

        # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
        expiry_date = 'GTC'
        if order.valid in [Order.DAY, 0]:
            expiry_date = 'TODAY'
        elif isinstance(order.valid, date):  # Если заявка поставлена до даты
            expiry_date = order.valid.strftime('%Y%m%d')

        if order.info['stop_order_kind'] == 'TAKE_PROFIT_STOP_ORDER':
            min_price_step = order.info['min_price_step']

            trans['STOP_ORDER_KIND'] = order.info['stop_order_kind']
            # Единицы измерения (защитного спрэда/отступа) в параметрах цены
            # Шаг изменения равен шагу цены по данному инструменту
            trans['SPREAD_UNITS'] = trans['OFFSET_UNITS'] = 'PRICE_UNITS'
            # Размер защитного спрэда. Размер отступа.
            # Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
            trans['SPREAD'] = trans['OFFSET'] = f'{min_price_step:.{scale}f}'
        else:  # Для обычных стоп заявок
            # Лимитная цена исполнения
            trans['PRICE'] = f'{plimit:.{scale}f}'

        trans['ACTION'] = 'NEW_STOP_ORDER'
        trans['STOPPRICE'] = f'{price:.{scale}f}'  # Стоп цена срабатывания
        trans['EXPIRY_DATE'] = expiry_date  # Срок действия стоп заявки
