import pandas as pd
from orders import Order, Client, Instrument

class Checking:
    def checking(self, order: Order, clients_df, instruments_df, rejected: list, completed_buys): 
        
        order_id = order.get_order_id()
        instrument_id = order.get_instrument_id()
        client_id = order.get_client_id()

        # check if instrument is missing 
        if instrument_id == None: 
            rejected.append([order_id, "REJECTED – INSTRUMENT NOT FOUND"])
            return False

        # check if currency of instrument is part of client's declared currencies
        currencies_accepted = clients_df[client_id].get_currencies().split(",")
        if instruments_df[instrument_id].get_currency() not in currencies_accepted:
            rejected.append([order_id, "REJECTED – MISMATCH CURRENCY"])
            return False
            
        # check lot size
        if order.get_quantity() % instruments_df[instrument_id].get_lot_size() != 0: 
            rejected.append([order_id, "REJECTED – INVALID LOT SIZE"])
            return False
            

        # Check position
        if clients_df[client_id].get_position_check() =="Y" and order.get_side() == "Sell": 
            sell_size = order.get_quantity()

            # check hash map
            if (client_id, instrument_id) in completed_buys:
                print("--------- COMPLETED ______", completed_buys) 
                position = completed_buys[client_id, instrument_id]

                if sell_size >= position:  
                    rejected.append([order.order_id, "REJECTED – POSITION CHECK FAILED"])
                    return False
                    
            else: 
                rejected.append([order.order_id, "REJECTED – POSITION CHECK FAILED"])
                return False
            
        return True          
