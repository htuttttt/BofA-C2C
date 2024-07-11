import pandas as pd

class Export:
    def client_report(self, trades: list):
        # from trades, calculate the positions of each client
        # convert to pandas dataframe
        # convert to csv
        client_report = {}
        
        for trade in trades:
            if (trade[2], trade[4]) not in client_report:
                client_report[trade[2], trade[4]] = trade[6]
            else:
                client_report[trade[2], trade[4]] += trade[6]
            
            if (trade[3], trade[4]) not in client_report:
                client_report[trade[3], trade[4]] = -trade[6]
            else:
                client_report[trade[3], trade[4]] -= trade[6]

        client_report_list = []

        for client in client_report:
            client_report_list.append([client[0], client[1], client_report[client]])
        
        df = pd.DataFrame(client_report_list, columns=['Client ID', 'Instrument ID', 'Net Position'])
        df['Net Position'] = df['Net Position'].astype(int)
        df.to_csv('my_output_client_report.csv', index=False)


    def exchange_report(self, rejected: list):
        # from rejected, convert to df, then convert to csv

        df = pd.DataFrame(rejected, columns=['Order Id', 'Rejection Reason'])
        df.to_csv('my_output_exchange_report.csv', index=False)

    def instrument_report(self, open_prices, close_prices, traded: list):

        trading_prices = {}
        instruments = []
        for trade in traded:
            if trade[4] not in trading_prices:
                instruments.append(trade[4])
                trading_prices[trade[4]] = []
            if float(trade[5]) not in trading_prices[trade[4]]:
                trading_prices[trade[4]].append(float(trade[5]))
        
        day_high = {}
        day_low = {}
        for instrument in trading_prices:
            day_high[instrument] = max(trading_prices[instrument])
            day_low[instrument] = min(trading_prices[instrument])
        
        def vwap(trades, instrument):
            matched_priceXvol = 0
            matched_vol = 0 
            for trade in trades:
                if trade[4] == instrument:
                    matched_priceXvol += (trade[6] * float(trade[5]))
                    matched_vol += trade[6]
            
            vwap = round(matched_priceXvol / matched_vol, 4) if matched_vol != 0 else 0
            return vwap

        def total_volume(trades, instrument):
            total_volume = 0
            for trade in trades:
                if trade[4] == instrument:
                    total_volume += trade[6]
            return total_volume    

        # Instrument ID,Open Price,Close Price,Total Volume,VWAP,Day High,Day Low
        instrument_report = []
        for instrument in instruments:
            if len(open_prices) <= 0 or open_prices[instrument] == None:
                open = 'NULL'
            else:
                open = open_prices[instrument]
            if len(close_prices) <= 0 or close_prices[instrument] == None:
                close = 'NULL'
            else:
                close = close_prices[instrument]
            volume = total_volume(traded, instrument)
            VWAP = vwap(traded, instrument)
            high = day_high[instrument]
            low = day_low[instrument]

            instrument_report.append([instrument, open, close, volume, VWAP, high, low])
        
        df = pd.DataFrame(instrument_report, columns=['Instrument ID','Open Price','Close Price','Total Volume','VWAP','Day High','Day Low'])
        df.to_csv('my_output_instrument_report.csv', index=False)

