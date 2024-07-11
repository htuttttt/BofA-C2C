import heapq
from datetime import datetime as dt

clients = {}

class Client:
    def __init__(self, client_id, currencies, position_check, rating):
        self.client_id = client_id
        self.currencies = currencies
        self.position_check = position_check
        self.rating = rating

    def get_currencies(self):
        return self.currencies

    def get_position_check(self):
        return self.position_check

class Instrument:
    def __init__(self, instrument_id, currency, lot_size):
        self.instrument_id = instrument_id
        self.currency = currency
        self.lot_size = lot_size
    
    def get_instrument_id(self):
        return self.instrument_id
    def get_currency(self):
        return self.currency
    def get_lot_size(self):
        return self.lot_size

    
def _format_time(arrival_time):
    return dt.strptime(arrival_time, "%H:%M:%S")

class Order:
    def __init__(self, time, order_id, instrument_id, quantity, client_id, price, side, clients_df):
        self.order_id = order_id
        self.client_id = client_id
        self.instrument_id = instrument_id
        self.quantity = quantity
        self.side = side
        self.price = price
        self.time = _format_time(time)
        self.rating = clients_df[client_id].rating

    def get_instrument_id(self):
        return self.instrument_id
    def get_order_id(self):
        return self.order_id
    def get_side(self):
        return self.side
    def get_client_id(self):
        return self.client_id
    def get_quantity(self):
        return self.quantity
    def get_price(self):
        return self.price


    def __lt__(self, other):
        if self.price == "Market" and other.price != "Market":
            return True
        if self.price != "Market" and other.price == "Market":
            return False

        if self.price == other.price:
            if self.rating == other.rating:
                return self.time < other.time
            else:
                return self.rating < other.rating
        else:
            if self.side == "Buy": # buy
                return self.price > other.price
            else: # sell
                return self.price < other.price

        # if self.side == "Sell":
        #     return (float(self.price), self.rating, self.time) < (
        #         float(other.price),
        #         other.rating,
        #         other.time,
        #     )
        # else:  # For buy side, we want higher priority for orders with a higher price.
        #     return (-float(self.price), self.rating, self.time) < (
        #         -float(other.price),
        #         other.rating,
        #         other.time,
        #     )
        
    def __gt__(self, obj):
        """self > obj."""
        if self.price == "Market" and obj.price != "Market":
            return False
        if self.price != "Market" and obj.price == "Market":
            return True

        if self.price == obj.price:
            if self.rating == obj.rating:
                return self.time > obj.time
            else:
                return self.rating > obj.rating
        else:
            if self.side: # buy
                return self.price < obj.price
            else: # sell
                return self.price > obj.price

    def __repr__(self):
        return f"{self.order_id, self.client_id, self.instrument_id, self.quantity, self.side, self.price, self.time.strftime('%H:%M:%S'), self.rating}"