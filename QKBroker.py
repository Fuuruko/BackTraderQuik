import collections
import time
from datetime import date, datetime

from backtrader import MetaBroker, BuyOrder, Order, SellOrder
from backtrader.position import Position


class QKBroker(metaclass=MetaBroker):
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

        self.notifs = collections.deque()  # Очередь уведомлений брокера о заявках

        # Стартовые и текущие свободные средства по счету
        self.startingcash = self.cash = 0
        # Стартовый и текущий баланс счета
        self.startingvalue = self.value = 0

        # Для брокера Финам нужно вместо кода клиента
        if not self.p.order_client_code:
            # указать номер торгового терминала
            self.p.order_client_code = self.p.client_code

        # Список номеров сделок по тикеру для фильтрации дублей сделок
        self.trade_nums = dict()
        self.positions = collections.defaultdict(Position)  # Список позиций
        # Список заявок, отправленных на биржу
        self.orders = collections.OrderedDict()

        # Список связанных заявок (One Cancel Others)
        self.ocos = {}
        # Очередь всех родительских/дочерних заявок (Parent - Children)
        self.pcs = collections.defaultdict(collections.deque)

    def start(self):
        super().start()
        # Ответ на транзакцию пользователя
        self.prov.OnTransReply = self.on_trans_reply
        # Получение новой / изменение существующей сделки
        self.prov.OnTrade = self.on_trade

        # Если нужно при запуске брокера получить текущие позиции на бирже
        if self.p.use_positions:
            self.get_all_active_positions()
        # Стартовые и текущие свободные средства по счету
        self.startingcash = self.cash = self.getcash()
        # Стартовый и текущий баланс счета
        self.startingvalue = self.value = self.getvalue()

    def getcash(self):
        """Свободные средства по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        # Свободные средства по счету
        cash = self.get_money_limits()
        if cash:  # Если свободные средства были получены
            self.cash = cash  # то запоминаем их
        return self.cash

    def getvalue(self, datas=None):
        """Стоимость позиций по счету"""
        # TODO Если не находимся в режиме Live, то не делать запросы
        # TODO Выдавать баланс по тикерам (datas) как в Alor
        # TODO Выдавать весь баланс, если не указаны параметры. Иначе, выдавать баланс по параметрам

        # Стоимость позиций по счету
        value = self.get_positions_limits()
        if value:  # Если стоимость позиций была получена
            self.value = value  # Баланс счета = свободные средства + стоимость позиций
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
        return self.positions[data._name]

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, oco=None, trailamount=None,
            trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на покупку"""
        order = self.create_order(owner, data, size, price, plimit, exectype,
                                  valid, oco, parent, transmit, is_buy=True,
                                  client_code=self.p.order_client_code,
                                  trade_acc_id=self.p.trade_acc_id, **kwargs)
        # Уведомляем брокера об отправке новой заявки на покупку на биржу
        self.notifs.append(order.clone())
        return order

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, oco=None, trailamount=None,
             trailpercent=None, parent=None, transmit=True, **kwargs):
        """Заявка на продажу"""
        order = self.create_order(owner, data, size, price, plimit, exectype,
                                  valid, oco, parent, transmit, is_buy=False,
                                  client_code=self.p.order_client_code,
                                  trade_acc_id=self.p.trade_acc_id, **kwargs)
        # Уведомляем брокера об отправке новой заявки на продажу на биржу
        self.notifs.append(order.clone())
        return order

    def cancel(self, order):
        """Отмена заявки"""
        return self.cancel_order(order)

    def get_notification(self):
        if not self.notifs:
            return None
        # Удаляем и возвращаем крайний левый элемент списка уведомлений
        return self.notifs.popleft()

    def next(self):
        self.notifs.append(None)  # mark notificatino boundary

    def stop(self):
        super().stop()
        (
            self.prov.OnConnected,     # Соединение терминала с сервером QUIK
            self.prov.OnDisconnected,  # Отключение терминала от сервера QUIK
            self.prov.OnTransReply,    # Ответ на транзакцию пользователя
            self.prov.OnTrade,         # Получение новой / изменение существующей сделки
        ) = (self.prov.default_handler,) * 4

        # Удаляем класс брокера из хранилища
        self.store.BrokerCls = None

    # Функции

    def get_all_active_positions(self):
        """Все активные позиции по счету"""
        if self.p.is_futures:  # Для фьючерсов свои расчеты
            class_code = 'SPBFUT'  # Код площадки
            # Все фьючерсные позиции
            futur_holds = self.prov.getFuturesHolding()
            # Активные фьючерсные позиции
            active_futur_holds = [f_h for f_h in futur_holds if not f_h['totalnet']]

            # Пробегаемся по всем активным фьючерсным позициям
            for act_futur_hold in active_futur_holds:
                sec_code = act_futur_hold['sec_code']  # Код тикера
                # Получаем название тикера по коду площадки и коду тикера
                ticker = f'{class_code}.{sec_code}'
                lots = act_futur_hold['totalnet']  # Кол-во
                if not self.p.is_lots:  # Если входящий остаток в штуках то переводим в лоты
                    lots = self.size_to_lots(class_code, sec_code, lots)
                # Цена приобретения
                price = float(act_futur_hold['avrposnprice'])
                # Сохраняем в списке открытых позиций
                self.positions[ticker] = Position(lots, price)

        else:  # Для остальных фирм
            # Все лимиты по бумагам (позиции по инструментам)
            depo_limits = self.prov.get_depo_limits()
            acc_depo_limits = [depo_limit for depo_limit in depo_limits  # Бумажный лимит
                               # выбираем по коду клиента
                               if depo_limit['client_code'] == self.p.client_code
                               and depo_limit['firmid'] == self.p.firm_id  # фирме
                               and depo_limit['limit_kind'] == self.p.limit_kind  # вид лимита
                               and depo_limit['currentbal'] != 0]  # только открытые позиции
            # TODO: change name
            for firm_kind_depo_limit in acc_depo_limits:
                # В позициях код тикера указывается без кода площадки
                sec_code = firm_kind_depo_limit['sec_code']
                # По коду тикера без площадки получаем код площадки и код тикера
                class_code, sec_code = self.store.from_ticker(sec_code)
                lots = int(firm_kind_depo_limit['currentbal'])  # Кол-во
                if not self.p.is_lots:  # Если входящий остаток в штуках то переводим в лоты
                    lots = self.size_to_lots(class_code, sec_code, lots)
                # Цена приобретения
                price = float(firm_kind_depo_limit['wa_position_price'])
                # Получаем название тикера по коду площадки и коду тикера
                ticker = f'{class_code}.{sec_code}'
                # Сохраняем в списке открытых позиций
                self.positions[ticker] = Position(lots, price)

    def get_money_limits(self):
        """Свободные средства по счету или None"""
        client_code = self.p.client_code
        firm_id = self.p.firm_id
        trade_account_id = self.p.trade_acc_id
        limit_kind = self.p.limit_kind
        currency_code = self.p.curr_code

        if self.p.is_futures:  # Для фьючерсов свои расчеты
            # Видео: https://www.youtube.com/watch?v=u2C7ElpXZ4k
            # Баланс = Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            # Лимит откр.поз. = Сумма, которая была на счету вчера в 19:00 МСК (после вечернего клиринга)
            # Вариац.маржа = Рассчитывается с 19:00 предыдущего дня без учета комисии.
            #   Перейдет в Накоплен.доход и обнулится в 14:00 (на дневном клиринге)
            # Накоплен.доход включает Биржевые сборы
            # Тек.чист.поз. = Заблокированное ГО под открытые позиции
            # План.чист.поз. = На какую сумму можете открыть еще позиции

            # Фьючерсные лимиты
            futures_limit = self.prov.getFuturesLimit(firm_id, trade_account_id, 0, 'SUR')
            # Лимит откр.поз. + Вариац.маржа + Накоплен.доход
            if futures_limit:
                return (futures_limit['cbplimit']
                        + futures_limit['varmargin']
                        + futures_limit['accruedint'])
            else:
                print(f'QUIK не вернул фьючерсные лимиты с firm_id={firm_id}, '
                      f'trade_acc_id={trade_account_id}. Проверьте правильность значений')
                return None
        # Для остальных фирм
        # Все денежные лимиты (остатки на счетах)
        money_limits = self.prov.getMoneyLimits()
        if not money_limits:  # Если денежных лимитов нет
            print('QUIK не вернул денежные лимиты (остатки на счетах). Свяжитесь с брокером')
            return None
        cash = [money_limit for money_limit in money_limits  # Из всех денежных лимитов
                # выбираем по коду клиента
                if money_limit['client_code'] == client_code
                and money_limit['firmid'] == firm_id  # фирме
                and money_limit['limit_kind'] == limit_kind  # вид лимита
                and money_limit["currcode"] == currency_code]  # и валюте
        # NOTE: Could cash be more than 1?
        if not cash:  # Если ни один денежный лимит не подходит
            print(f'Денежный лимит не найден с client_code={client_code}, '
                  f'firm_id={firm_id}, limit_kind={limit_kind}, '
                  f'curr_code={currency_code}. Проверьте правильность значений')
            # Для отладки, если нужно разобраться, что указано неверно
            # print(f'Полученные денежные лимиты: {money_limits}')
            return None
        # Денежный лимит (остаток) по счету
        return float(cash[0]['currentbal'])

    def get_positions_limits(self):
        """Стоимость позиций по счету"""
        if self.p.is_futures:  # Для фьючерсов свои расчеты
            # Фьючерсные лимиты
            futures_limit = self.prov.getFuturesLimit(self.p.firm_id, 
                                                      self.p.trade_acc_id,
                                                      0, 'SUR')
            if futures_limit:
                # Тек.чист.поз. (Заблокированное ГО под открытые позиции)
                return futures_limit['cbplused']
            else:
                return None

        # Для остальных фирм
        pos_value = 0  # Стоимость позиций по счету
        # Пробегаемся по копии позиций (чтобы не было ошибки при изменении позиций)
        for dataname in self.positions.keys():
            # По названию тикера получаем код площадки и код тикера
            class_code, sec_code = self.store.from_ticker(dataname)
            # Последняя цена сделки
            last_price = self.prov.getParamEx(class_code, sec_code, 'LAST')
            last_price = float(last_price['param_value'])

            pos = self.positions[dataname]  # Получаем позицию по тикеру
            pos_value += pos.size * last_price  # Добавляем стоимость позиции
        return pos_value  # Стоимость позиций по счету

    def create_order(self, owner, data, size, price=None, plimit=None,
                     exectype=None, valid=None, oco=None, parent=None,
                     transmit=True, is_buy=True, **kwargs):
        """Создание заявки.

        Привязка параметров счета и тикера.
        Обработка связанных и родительской/дочерних заявок
        """
        # Заявка на покупку/продажу
        if is_buy:
            order = BuyOrder(owner=owner, data=data, size=size, price=price,
                             pricelimit=plimit, exectype=exectype, valid=valid,
                             oco=oco, parent=parent, transmit=transmit)
        else:
            order = SellOrder(owner=owner, data=data, size=size, price=price,
                              pricelimit=plimit, exectype=exectype, valid=valid,
                              oco=oco, parent=parent, transmit=transmit)

        # По тикеру выставляем комиссии в заявку. Нужно для исполнения заявки в BackTrader
        order.addcomminfo(self.getcommissioninfo(data))
        # Передаем в заявку все дополнительные свойства из брокера,
        # в т.ч. client_code, trade_acc_id, stop_order_kind
        order.addinfo(**kwargs)
        # Из названия тикера получаем код площадки и тикера
        class_code, sec_code = self.store.from_ticker(data._name)
        # Код площадки class_code и тикера sec_code
        order.addinfo(class_code=class_code, sec_code=sec_code)

        # Получаем параметры тикера (min_price_step, scale)
        si = self.store.get_symbol_info(class_code, sec_code)
        if not si:  # Если тикер не найден
            print(f'Постановка заявки {order.ref} по тикеру '
                  f'{class_code}.{sec_code} отменена. Тикер не найден')
            order.reject(self)  # то отменяем заявку (статус Order.Rejected)
            return order  # Возвращаем отмененную заявку

        # Минимальный шаг цены
        order.addinfo(min_price_step=si['min_price_step'])
        # Размер проскальзывания в деньгах slippage
        order.addinfo(slippage=si['min_price_step'] * self.p.stop_steps)
        # Кол-во значащих цифр после запятой scale
        order.addinfo(scale=si['scale'])

        if oco:  # Если есть связанная заявка
            # то заносим в список связанных заявок
            self.ocos[order.ref] = oco.ref
        if not transmit or parent:  # Для родительской/дочерних заявок
            # Номер транзакции родительской заявки или номер заявки, если родительской заявки нет
            parent_ref = getattr(order.parent, 'ref', order.ref)
            # Если есть родительская заявка, но она не найдена в очереди родительских/дочерних заявок
            if order.ref != parent_ref and parent_ref not in self.pcs:
                print(f'Постановка заявки {order.ref} '
                      f'по тикеру {class_code}.{sec_code} отменена. '
                      f'Родительская заявка не найдена')
                order.reject(self)
                return order

            pcs = self.pcs[parent_ref]  # В очередь к родительской заявке
            pcs.append(order)  # добавляем заявку (родительскую или дочернюю)

        if transmit:  # Если обычная заявка или последняя дочерняя заявка
            if not parent:  # Для обычных заявок
                return self.place_order(order)  # Отправляем заявку на биржу
            else:  # Если последняя заявка в цепочке родительской/дочерних заявок
                # Уведомляем брокера о создании новой заявки
                self.notifs.append(order.clone())
                # Отправляем родительскую заявку на биржу
                return self.place_order(order.parent)
        # Если не последняя заявка в цепочке родительской/дочерних заявок (transmit=False)
        # то возвращаем созданную заявку со статусом Created. На биржу ее пока не ставим
        return order

    def place_order(self, order):
        """Отправка заявки (транзакции) на биржу"""
        class_code = order.info['class_code']  # Код площадки
        sec_code = order.info['sec_code']  # Код тикера

        price = order.price  # Цена заявки
        if not price:  # Если цена не указана для рыночных заявок
            # Цена рыночной заявки должна быть нулевой (кроме фьючерсов)
            price = 0.00
        slippage = order.info['slippage']  # Размер проскальзывания в деньгах

        # TODO: Unnecessary translation to int
        # Целое значение проскальзывания мы должны отправлять без десятичных знаков
        if slippage.is_integer():
            # поэтому, приводим такое проскальзывание к целому числу
            slippage = int(slippage)

        if order.exectype == Order.Market:  # Для рыночных заявок
            if class_code == 'SPBFUT':  # Для рынка фьючерсов
                # Последняя цена сделки
                last_price = self.prov.getParamEx(class_code, sec_code, 'LAST')['param_value']
                last_price = float(last_price)
                # Из документации QUIK: При покупке/продаже фьючерсов
                # по рынку нужно ставить цену хуже последней сделки
                price = last_price + (slippage if order.isbuy() else -slippage)

        scale = order.info['scale']  # Кол-во значащих цифр после запятой
        price = round(price, scale)  # Округляем цену до кол-ва значащих цифр
        if price.is_integer():  # Целое значение цены мы должны отправлять без десятичных знаков
            price = int(price)  # поэтому, приводим такую цену к целому числу
        transaction = {  # Все значения должны передаваться в виде строк
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            # Код клиента. Для фьючерсов его нет
            'CLIENT_CODE': order.info['client_code'],
            'ACCOUNT': order.info['trade_acc_id'],  # Счет
            'CLASSCODE': class_code,  # Код площадки
            'SECCODE': sec_code,  # Код тикера
            'OPERATION': 'B' if order.isbuy() else 'S',  # B = покупка, S = продажа
            'PRICE': str(price),  # Цена исполнения
            'QUANTITY': str(abs(order.size))}  # Кол-во в лотах

        if order.exectype in [Order.Stop, Order.StopLimit]:  # Для стоп заявок
            transaction['ACTION'] = 'NEW_STOP_ORDER'  # Новая стоп заявка
            transaction['STOPPRICE'] = str(price)  # Стоп цена срабатывания
            plimit = order.pricelimit  # Лимитная цена исполнения

            if plimit:  # Если задана лимитная цена исполнения
                # то ее и берем, округлив цену до кол-ва значащих цифр
                limit_price = round(plimit, scale)
            else:  # Если цена не задана
                # то будем покупаем/продаем по большей/меньшей цене в размер проскальзывания
                limit_price = price + (slippage if order.isbuy() else -slippage)
            # По умолчанию будем держать заявку до отмены GTC = Good Till Cancelled
            expiry_date = 'GTC'

            if order.valid in [Order.DAY, 0]:  # Если заявка поставлена на день
                expiry_date = 'TODAY'  # то будем держать ее до окончания текущей торговой сессии
            elif isinstance(order.valid, date):  # Если заявка поставлена до даты
                # то будем держать ее до указанной даты
                expiry_date = order.valid.strftime('%Y%m%d')
            # Срок действия стоп заявки
            transaction['EXPIRY_DATE'] = expiry_date

            # Если тип стоп заявки это тейк профит
            if order.info['stop_order_kind'] == 'TAKE_PROFIT_STOP_ORDER':
                # Минимальный шаг цены
                min_price_step = order.info['min_price_step']
                # Тип заявки TAKE_PROFIT_STOP_ORDER
                transaction['STOP_ORDER_KIND'] = order.info['stop_order_kind']
                # Единицы измерения (защитного спрэда/отступа) в параметрах цены
                # Шаг изменения равен шагу цены по данному инструменту
                transaction['SPREAD_UNITS'] = transaction['OFFSET_UNITS'] = 'PRICE_UNITS'
                # Размер защитного спрэда. Размер отступа.
                # Переводим в строку, чтобы избежать научной записи числа шага цены. Например, 5e-6 для ВТБ
                transaction['SPREAD'] = transaction['OFFSET'] = f'{min_price_step:.{scale}f}'
            else:  # Для обычных стоп заявок
                # Лимитная цена исполнения
                transaction['PRICE'] = str(limit_price)
        else:  # Для рыночных или лимитных заявок
            # Новая рыночная или лимитная заявка
            transaction['ACTION'] = 'NEW_ORDER'
            # L = лимитная заявка (по умолчанию), M = рыночная заявка
            transaction['TYPE'] = 'L' if order.exectype == Order.Limit else 'M'
        response = self.prov.sendTransaction(transaction)  # Отправляем транзакцию на биржу
        # Отправляем заявку на биржу (статус Order.Submitted)
        order.submit(self)

        # Если возникла ошибка при постановке заявки на уровне QUIK
        if 'lua_error' in response:
            # то заявка не отправляется на биржу, выводим сообщение об ошибке
            print(f'Ошибка отправки заявки в QUIK '
                  f'{response["CLASSCODE"]}.{response["SECCODE"]} '
                  f'{response["lua_error"]}')
            order.reject(self)
        # Сохраняем в списке заявок, отправленных на биржу
        self.orders[order.ref] = order
        return order

    def cancel_order(self, order):
        """Отмена заявки"""
        if not order.alive():  # Если заявка уже была завершена
            return None
        if order.ref not in self.orders:  # Если заявка не найдена
            return None
        order_num = order.info['order_num']  # Номер заявки на бирже
        # Задана стоп заявка и лимитная заявка не выставлена
        is_stop = (order.exectype in [Order.Stop, Order.StopLimit]
                   and self.prov.get_order_by_num(order_num))
        transaction = {
            'TRANS_ID': str(order.ref),  # Номер транзакции задается клиентом
            'CLASSCODE': order.info['class_code'],
            'SECCODE': order.info['sec_code']}
        if is_stop:  # Для стоп заявки
            transaction['ACTION'] = 'KILL_STOP_ORDER'
            transaction['STOP_ORDER_KEY'] = str(order_num)
        else:  # Для лимитной заявки
            transaction['ACTION'] = 'KILL_ORDER'
            transaction['ORDER_KEY'] = str(order_num)
        self.prov.sendTransaction(transaction)
        # В список уведомлений ничего не добавляем. Ждем события OnTransReply
        return order

    def oco_pc_check(self, order):
        """Проверка связанных заявок. Проверка родительской/дочерних заявок"""
        for order_ref, oco_ref in self.ocos.items():  # Пробегаемся по списку связанных заявок
            # Если в заявке номер эта заявка указана как связанная (по номеру транзакции)
            if oco_ref == order.ref:
                self.cancel_order(self.orders[order_ref])  # то отменяем заявку
        if order.ref in self.ocos.keys():  # Если у этой заявки указана связанная заявка
            # то получаем номер транзакции связанной заявки
            oco_ref = self.ocos[order.ref]
            # отменяем связанную заявку
            self.cancel_order(self.orders[oco_ref])

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

    def on_trans_reply(self, trans_reply):
        """Обработчик события ответа на транзакцию пользователя"""
        trans_id = trans_reply['trans_id']
        # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
        if trans_id == 0:
            return None
        order_num = int(trans_reply['order_num'])  # Номер заявки на бирже
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            print(f'Заявка {order_num} на бирже с номером транзакции {trans_id} не найдена')
            return None
        # Ищем заявку по номеру транзакции
        order: Order = self.orders[trans_id]
        order.addinfo(order_num=order_num)  # Сохраняем номер заявки на бирже

        result_msg = str(trans_reply['result_msg']).lower()
        status = int(trans_reply['status'])
        # TODO: status == 3 mean accepted as well
        # TODO: analyze by status
        # NOTE: This error may occur because broker start before data come
        # TODO В BT очень редко при order.cancel(), order.reject(), order.margin() возникает ошибка:
        #    order.py, line 492, in margin
        #    self.executed.dt = self.data.datetime[0]
        #    linebuffer.py, line 163, in __getitem__
        #    return self.array[self.idx + ago]
        #    IndexError: array index out of range
        if status == 15 or 'зарегистрирован' in result_msg:  # Если пришел ответ по новой заявке
            order.accept(self)  # Заявка принята на бирже (Order.Accepted)
        elif 'снят' in result_msg:  # Если пришел ответ по отмене существующей заявки
            try:
                order.cancel()  # Отменяем существующую заявку (Order.Canceled)
            # except Exception:  # При ошибке
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Canceled  # все равно ставим статус заявки Order.Canceled
                # print('Ошибка:\n', traceback.format_exc())

        # Транзакция не выполнена (ошибка заявки):
        elif status in (2, 4, 5, 10, 11, 12, 13, 14, 16):
            # - Не найдена заявка для удаления
            # - Вы не можете снять данную заявку
            # - Превышен лимит отправки транзакций для данного логина
            if (status == 4 and 'не найдена заявка' in result_msg
                    or status == 5 and 'не можете снять' in result_msg
                    or 'превышен лимит' in result_msg):
                return None
            try:
                order.reject(self)  # Отклоняем заявку (Order.Rejected)
            # except Exception:  # При ошибке
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Rejected  # все равно ставим статус заявки Order.Rejected
                # print('Ошибка:\n', traceback.format_exc())

        elif status == 6:  # Транзакция не прошла проверку лимитов сервера QUIK
            try:
                order.margin()  # Для заявки не хватает средств (Order.Margin)
            # except Exception:
            except (KeyError, IndexError):  # При ошибке
                order.status = Order.Margin  # все равно ставим статус заявки Order.Margin
                # print('Ошибка:\n', traceback.format_exc())

        self.notifs.append(order.clone())  # Уведомляем брокера о заявке
        if order.status != Order.Accepted:  # Если новая заявка не зарегистрирована
            # то проверяем связанные и родительскую/дочерние заявки (Canceled, Rejected, Margin)
            self.oco_pc_check(order)

    def on_trade(self, trade):
        """Обработчик события получения новой / изменения существующей сделки.

        Выполняется до события изменения существующей заявки.
        Нужен для определения цены исполнения заявок.
        """
        order_num = int(trade['order_num'])  # Номер заявки на бирже
        # По номеру заявки в сделке пробуем получить заявку с биржи
        order = self.prov.get_order_by_num(order_num)
        # Если заявка не найдена, то в ответ получаем целое число номера заявки.
        # Возможно заявка есть, но она не успела прийти к брокеру
        # TODO: Is this even possible?
        if not order:
            print(f'Заявка с номером {order_num} не найдена на бирже с 1-ой попытки. '
                  f'Через 3с будет 2-ая попытка')
            time.sleep(3)
            # Снова пробуем получить заявку с биржи по ее номеру
            order = self.prov.get_order_by_num(order_num)
            if not order:
                print(f'Заявка с номером {order_num} не найдена на бирже со 2-ой попытки')
                return None

        # Получаем номер транзакции из заявки с биржи
        trans_id = int(order['trans_id'])
        # Заявки, выставленные не из автоторговли / только что (с нулевыми номерами транзакции)
        if trans_id == 0:
            return None
        if trans_id not in self.orders:  # Пришла заявка не из автоторговли
            print(f'Заявка с номером {order_num} и номером транзакции '
                  f'{trans_id} была выставлена не из торговой системы')
            return None
        # Ищем заявку по номеру транзакции
        order: Order = self.orders[trans_id]
        # Сохраняем номер заявки на бирже (может быть переход от стоп заявки к лимитной с изменением номера на бирже)
        order.addinfo(order_num=order_num)

        class_code = trade['class_code']  # Код площадки
        sec_code = trade['sec_code']  # Код тикера

        ticker = f'{class_code}.{sec_code}'
        # Номер сделки (дублируется 3 раза)
        trade_num = int(trade['trade_num'])
        if ticker not in self.trade_nums.keys():  # Если это первая сделка по тикеру
            self.trade_nums[ticker] = []  # то ставим пустой список сделок
        # Если номер сделки есть в списке (фильтр для дублей)
        elif trade_num in self.trade_nums[ticker]:
            return None

        # Запоминаем номер сделки по тикеру, чтобы в будущем ее не обрабатывать (фильтр для дублей)
        self.trade_nums[ticker].append(trade_num)
        lots = int(trade['qty'])  # Абсолютное кол-во
        if not self.is_lots:  # Если входящий остаток в штуках то переводим в лоты
            lots = self.size_to_lots(class_code, sec_code, lots)
        # Если сделка на продажу (бит 2)
        if trade['flags'] & 0b100 == 0b100:
            lots *= -1  # то кол-во ставим отрицательным
        price = float(trade['price'])
        # NOTE: This error may occur because broker start before data come
        # TODO Очень редко возникает ошибка:
        #    linebuffer.py, line 163, in __getitem__
        #    return self.array[self.idx + ago]
        #    IndexError: array index out of range
        try:
            # Дата и время исполнения заявки. Последняя известная
            dt = order.data.datetime[0]
        # except Exception:  # При ошибке
        except (KeyError, IndexError):  # При ошибке
            # Берем текущее время на бирже из локального
            dt = datetime.now(self.store.MarketTimeZone)
            # print('Ошибка:\n', traceback.format_exc())

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

        # Уведомляем брокера о исполнении заявки
        self.notifs.append(order.clone())

        # Если заявка исполнена полностью (ничего нет к исполнению)
        if order.status == order.Completed:
            # Снимаем oco-заявку только после полного исполнения заявки
            # Если нужно снять oco-заявку на частичном исполнении, то прописываем это правило в ТС
            # Проверяем связанные и родительскую/дочерние заявки (Completed)
            self.oco_pc_check(order)

    def size_to_lots(self, class_code, sec_code, size: int):
        """Перевод кол-ва из штук в лоты"""
        # Получаем параметры тикера (lot_size)
        si = self.store.get_symbol_info(class_code, sec_code)
        if not si:  # Если тикер не найден
            return size
        lot_size = si['lot_size']
        return size // lot_size if lot_size > 0 else size
