import heapq
from orders import Order
from datetime import datetime as dt

class OrderQueue:
    def __init__(self):
        self.buy_heap = []
        self.sell_heap = []
        heapq.heapify(self.buy_heap)
        heapq.heapify(self.sell_heap)

    def get_buy_count(self):
        return len(self.buy_heap)

    def get_sell_count(self):
        return len(self.sell_heap)

    def add_buy_order(self, new_buy):
        heapq.heappush(self.buy_heap, new_buy)

    def add_sell_order(self, new_sell):
        heapq.heappush(self.sell_heap, new_sell)

    def get_best_buy_order(self):
        return heapq.heappop(self.buy_heap)

    def get_best_sell_order(self):
        return heapq.heappop(self.sell_heap)

    def print_heaps(self):
        print("Buy Heap:", self.buy_heap)
        print("Sell Heap:", self.sell_heap)


class Matching:

    # all buy and sell orders in match are assumed to be of same currency and instrument
    def match(self, buy: Order, sell: Order, traded: list, new_trades: list, order_queues: dict): 
        # PRICE
        
        if buy.price == "Market" and sell.price == "Market":
            return 1 # Both market prices, no trade occurred
        
        trading_price = None
        if buy.price == "Market":
            trading_price = sell.price

        elif sell.price == "Market":
            trading_price = buy.price
        
        elif float(sell.price) <= float(buy.price):
            if sell.time <= buy.time:
                trading_price = sell.price
            elif sell.time > buy.time:
                trading_price = buy.price

        if trading_price is None:
            return 2 # Sell price is higher than buy price, no trade occurred

        # QUANTITY
        trading_quantity = min(buy.quantity, sell.quantity)
        buy.quantity -= trading_quantity
        sell.quantity -= trading_quantity

        if buy.quantity > 0:
            order_queues[buy.instrument_id].add_buy_order(buy)
        if sell.quantity > 0:
            order_queues[sell.instrument_id].add_sell_order(sell)

        # record all trades that occurred if there is one
        traded.append([buy.order_id, sell.order_id, buy.client_id, sell.client_id, buy.instrument_id, trading_price, trading_quantity]) 
        new_trades.append([buy.order_id, sell.order_id, buy.client_id, sell.client_id, buy.instrument_id, trading_price, trading_quantity])
        
        return 0

    # handles the sliding window of the 2 heaps and
    # ensures that only buy and sell orders of the same currency and instrument are passed through match()
    def matching_engine(self, instrument_id, traded: list, new_trades: list, order_queues):
        print("instrument_id", order_queues[instrument_id])
        buy_orders = order_queues[instrument_id]
        sell_orders = order_queues[instrument_id]

        add_back_to_buy = []
        add_back_to_sell = []
       
        while buy_orders.get_buy_count() > 0 and sell_orders.get_sell_count() > 0:
            buy = buy_orders.get_best_buy_order()
            sell = sell_orders.get_best_sell_order()

            result = self.match(buy, sell, traded, new_trades, order_queues)

            if result == 1: # both orders are market
                # iterate through sell heap to find first limit order

                temp_sell_orders = []
                while sell_orders.get_sell_count() > 0:
                    next_sell = sell_orders.get_best_sell_order()
                    if next_sell != "Market":
                        result = self.match(buy, next_sell, traded, new_trades, order_queues)
                        break
                    else:
                        temp_sell_orders.append(next_sell)

                for o in temp_sell_orders:
                    sell_orders.add_sell_order(o)

                
                temp_buy_orders = []
                # iterate through buy heap to find first limit order
                while buy_orders.get_buy_count() > 0:
                    next_buy = buy_orders.get_best_buy_order()
                    if next_buy != "Market":
                        result = self.match(next_buy, sell, traded, new_trades, order_queues)

                        break
                    else:
                        temp_buy_orders.append(next_buy)

                for o in temp_buy_orders:
                    buy_orders.add_buy_order(o)
                    

            if result == 2: # not matching
                add_back_to_buy.append(buy)
                add_back_to_sell.append(sell)
                break

        for i in add_back_to_buy:
            buy_orders.add_buy_order(i)
            
        for i in add_back_to_sell:
            sell_orders.add_sell_order(i)
        

        print(f"OrderQueue for {instrument_id}:")
        order_queues[instrument_id].print_heaps()

        return traded, new_trades, order_queues


    # find the price at which most trades will occur before market opens
    def find_open_price(self, instrument_id, before_market_trades: dict, order_queues: dict, limit_orders, market_orders):
        # iterate through prices of all trades
        # substitute Market orders with each price & count how many trades occur
        # choose the price at which most trades occur as the open price
        # print("LIMIT ORDERS", limit_orders)
        # print("MARKET ORDERS", market_orders)
        print("open price order queues", order_queues)
        buy_orders = order_queues[instrument_id]
        sell_orders = order_queues[instrument_id]

        add_back_to_buy = []
        add_back_to_sell = []
        new_trades = []
        traded = []
        
        no_of_trades = 0
        open_price = None

        open_price_candidates = {} # all possible open prices for each instrument
        if instrument_id not in open_price_candidates:
            open_price_candidates[instrument_id] = []

        if instrument_id in limit_orders:
            for l in limit_orders[instrument_id]:
                if l.price not in open_price_candidates[instrument_id]:
                    open_price_candidates[instrument_id].append(l.price)
                if l.side == "Buy":
                    order_queues[instrument_id].add_buy_order(l)
                else:
                    order_queues[instrument_id].add_sell_order(l)
        
        for price in open_price_candidates[instrument_id]:

            for m in market_orders[instrument_id]:
                m.price = price # substitute Market orders with open price candidate 
                if m.side == "Buy":
                    order_queues[instrument_id].add_buy_order(m)
                else:
                    order_queues[instrument_id].add_sell_order(m)
            
            # perform the trades with the newly substituted price
            while buy_orders.get_buy_count() > 0 and sell_orders.get_sell_count() > 0:
                buy = buy_orders.get_best_buy_order()
                sell = sell_orders.get_best_sell_order()
                result = self.match(buy, sell, traded, new_trades, order_queues)

                if result == 2: # not matching
                    add_back_to_buy.append(buy)
                    add_back_to_sell.append(sell)
                    break

            for i in add_back_to_buy:
                buy_orders.add_buy_order(i)
                
            for i in add_back_to_sell:
                sell_orders.add_sell_order(i)
            
            # print("OPEN PRICE CANDIDATES", open_price_candidates)
            # print("NEW PRICE", price)
            # print("NO OF TRADES", len(traded))

            # choose the price at which most trades occur as the open price
            if len(traded) > 0 and len(traded) > no_of_trades:
                no_of_trades = len(traded)
                open_price = price

            traded = []

        return open_price

    def find_close_price(self, instrument_id, after_market_trades: dict, order_queues: dict, limit_orders, market_orders):
        # iterate through prices of all trades
        # substitute Market orders with each price & count how many trades occur
        # choose the price at which most trades occur as the close price

        buy_orders = order_queues[instrument_id]
        sell_orders = order_queues[instrument_id]

        add_back_to_buy = []
        add_back_to_sell = []

        new_trades = []
        traded = []
        
        no_of_trades = 0
        close_price = None

        close_price_candidates = {} # all possible close prices for each instrument
        if instrument_id not in close_price_candidates:
            close_price_candidates[instrument_id] = []

        if instrument_id in limit_orders:
            for l in limit_orders[instrument_id]:
                if l.price not in close_price_candidates[instrument_id]:
                    close_price_candidates[instrument_id].append(l.price)
                
                if l.side == "Buy":
                    order_queues[instrument_id].add_buy_order(l)
                else:
                    order_queues[instrument_id].add_sell_order(l)
            

        for price in close_price_candidates[instrument_id]:

            for m in market_orders[instrument_id]:
                m.price = price # substitute Market orders with open price candidate 
                
                if m.side == "Buy":
                    order_queues[instrument_id].add_buy_order(m)
                else:
                    order_queues[instrument_id].add_sell_order(m)
            
            # perform the trades with the newly substituted price
            while buy_orders.get_buy_count() > 0 and sell_orders.get_sell_count() > 0:
                buy = buy_orders.get_best_buy_order()
                sell = sell_orders.get_best_sell_order()

                result = self.match(buy, sell, traded, new_trades, order_queues)

                if result == 2: # not matching
                    add_back_to_buy.append(buy)
                    add_back_to_sell.append(sell)
                    break

            for i in add_back_to_buy:
                buy_orders.add_buy_order(i)
                
            for i in add_back_to_sell:
                sell_orders.add_sell_order(i)
            
            # print("OPEN PRICE CANDIDATES", close_price_candidates)
            # print("NEW PRICE", price)
            # print("NO OF TRADES", traded)

            if len(traded) > 0 and len(traded) > no_of_trades:
                no_of_trades = len(traded)
                close_price = price

            traded = []

            # # choose the price at which most trades occur as the close price
            # traded_price = {}
            # for trade in after_market_trades[instrument_id]:
            #     if trade[5] not in traded_price:
            #         traded_price[trade[5]] = 1
            #     else:
            #         traded_price[trade[5]] += 1
            
            # for i in traded_price:
            #     if i == max(traded_price.values()):
            #         close_price = i

        return close_price
                



        

        

