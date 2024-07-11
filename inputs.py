import pandas as pd
from orders import Client, Order, Instrument
from matching import Matching, OrderQueue
from checking import Checking
from exports import Export
from datetime import datetime as dt
import copy

# example-set
clients = pd.read_csv('./DataSets/example-set/input_clients.csv')
orders = pd.read_csv('./DataSets/example-set/input_orders.csv')
instruments = pd.read_csv('./DataSets/example-set/input_instruments.csv')

# test-set
# clients = pd.read_csv('./DataSets/test-set/input_clients.csv')
# orders = pd.read_csv('./DataSets/test-set/input_orders.csv')
# instruments = pd.read_csv('./DataSets/test-set/input_instruments.csv')

def client(clients):
    c = {}
    for i in range(len(clients)):
        row = clients.iloc[i] # access ith row
        client_id = row["ClientID"]
        currencies = row["Currencies"]
        position_check = row["PositionCheck"]
        rating = row["Rating"]

        c[client_id] = Client(client_id, currencies, position_check, rating)
    return c

def instrument(instruments):
    inst = {}
    for i in range(len(instruments)):
        row = instruments.iloc[i] # access ith row
        instrument_id = row["InstrumentID"]
        currency = row["Currency"]
        lot_size = row["LotSize"]
        
        inst[instrument_id] = Instrument(instrument_id, currency, lot_size)
    return inst

def order(orders, clients_df):
    o = {}
    for i in range(len(orders)):
        row = orders.iloc[i] # access ith row
        time = row["Time"]
        order_id = row["OrderID"]
        instrument_id = row["Instrument"]
        quantity = row["Quantity"]
        client_id = row["Client"]
        price = row["Price"]
        side = row["Side"]

        o[order_id] = Order(time, order_id, instrument_id, quantity, client_id, price, side, clients_df)
    return o

clients_df = client(clients)
instruments_df = instrument(instruments)
orders_df = order(orders, clients_df)

def group_orders_by_time(orders_df):
    grouped_orders = {}
    for order in orders_df.values():
        arrival_time = order.time
        if arrival_time not in grouped_orders:
            grouped_orders[arrival_time] = []
        grouped_orders[arrival_time].append(order)
    return grouped_orders

orders_df = group_orders_by_time(orders_df)
print(orders_df)

def _format_time(arrival_time):
    return dt.strptime(arrival_time, "%H:%M:%S")

def main():
    matching = Matching()
    checking = Checking()
    export = Export()

    traded = [] # trades occurred
    rejected = [] # rejected orders
    order_queues = {} # to handle buy and sell heaps for each instrument 
    completed_buys = {} # (client_id, instrument_id): quantity
    
    open_prices = {}  # open_prices for each instrument
    market_orders = {"before": {}, "after": {}}  # market orders before and after market hours
    limit_orders = {"before": {}, "after": {}}  # limit orders before and after market hours
    market_trades = {"before": {}, "after": {}}  # trades occurred before and after market hours
    order_queues_period = {"before": {}, "after": {}}  # order queues before and after market hours
    close_prices = {}  # close_prices for each instrument

    # to initialise dictionaries for a new instrument 
    def init_instrument_dicts(instrument_id):
        if instrument_id not in market_orders["before"]:
            market_orders["before"][instrument_id] = []
            limit_orders["before"][instrument_id] = []
            market_orders["after"][instrument_id] = []
            limit_orders["after"][instrument_id] = []
            order_queues[instrument_id] = OrderQueue()
            order_queues_period["before"][instrument_id] = OrderQueue()
            order_queues_period["after"][instrument_id] = OrderQueue()


    for time in orders_df:
        orders_at_same_time = orders_df[time]
        valid_orders = []
        new_trades = []
        
        for order in orders_at_same_time:
            checking.checking(order, clients_df, instruments_df, rejected, completed_buys)
        
        # filter out rejected orders
        rejected_orders = [r[0] for r in rejected]
        valid_orders = [order for order in orders_at_same_time if order.get_order_id() not in rejected_orders]

        instrument_to_consider = set()

        for new_order in valid_orders:

            instrument_id = new_order.instrument_id
            instrument_to_consider.add(instrument_id)
            init_instrument_dicts(instrument_id)

            if new_order.side == "Buy":
                order_queues[instrument_id].add_buy_order(new_order)
            else:
                order_queues[instrument_id].add_sell_order(new_order)

            n = copy.deepcopy(new_order)

            if _format_time('9:00:00') <= new_order.time < _format_time('9:30:00'):
                if new_order.price == "Market":
                    market_orders["before"][instrument_id].append(n)
                else:
                    limit_orders["before"][instrument_id].append(n)
            elif _format_time('16:00:00') <= new_order.time:
                if new_order.price == "Market":
                    market_orders["after"][instrument_id].append(n)
                else:
                    limit_orders["after"][instrument_id].append(n)

        for inst in instrument_to_consider: 
            # Determine open_price for orders before market opens
            if _format_time('9:00:00') <= new_order.time < _format_time('9:30:00'):
                open_prices[instrument_id] = matching.find_open_price(inst, market_trades["before"], order_queues_period["before"], limit_orders["before"], market_orders["before"])

            # Determine close_price for orders when market closes
            if _format_time('16:00:00') <= new_order.time:
                close_prices[instrument_id] = matching.find_close_price(inst, market_trades["after"], order_queues_period["after"], limit_orders["after"], market_orders["after"])

            # Add orders to heaps and match them
            traded, new_trades, order_queues = matching.matching_engine(inst, traded, new_trades, order_queues)

            for new_trade in new_trades:
                buy_side_key = (new_trade[2], new_trade[4])
                sell_side_key = (new_trade[3], new_trade[4])
                qty = new_trade[6]

                completed_buys[buy_side_key] = completed_buys.get(buy_side_key, 0) + qty
                completed_buys[sell_side_key] = completed_buys.get(sell_side_key, 0) - qty

    print("----------- Rejected ---------- \n", rejected)
    print("----------- Traded -----------\n", traded)
    print("----------- Open Price -----------\n",open_prices)
    print("----------- Close Price -----------\n",close_prices)
    export.client_report(traded)
    export.exchange_report(rejected)
    export.instrument_report(open_prices, close_prices, traded)

main()