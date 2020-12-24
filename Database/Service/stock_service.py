from ta.utils import dropna

import Data.Technical_Data.Model.stock_model as stock_model
from Database.Service.database import database
import mysql.connector.errors as errors
from Database.Service.database import insert_error_log
import Data.Technical_Data.Model.simple_moving_average_model as sma_model
import pandas as pd
# from Utility.multiprocessor

class stock_service:
    def __init__(self):
        self.conn_stock = database().conn_stock


    def select_stock_obj(self):
        pass


    def select_all_stock_objs(self, ticker, dataframe = False, cleaned = True):
        sql_statement = f"SELECT * FROM stock_technical_data.STOCK_DATA where ticker={ticker} ORDER BY dt DESC;";
        try:
            cursor = self.conn_stock.cursor(dictionary=True)
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            stock_obj = self.parse_stock_obj(ticker, results)
            if dataframe:
                stock_obj = {
                    'symbol' : ticker,
                    'dt': [],
                    'open': [],
                    'close': [],
                    'high': [],
                    'low': [],
                    'adj_close': [],
                    'volume': [],
                    'dividend': [],
                    'split': []
                }
                for i in results:
                    stock_obj['dt'].append(i['dt'])
                    stock_obj['open'].append(i['open'])
                    stock_obj['close'].append(i['close'])
                    stock_obj['high'].append(i['high'])
                    stock_obj['low'].append(i['low'])
                    stock_obj['adj_close'].append(i['adj_close'])
                    stock_obj['volume'].append(i['volume'])
                    stock_obj['split'].append(i['split'])
                    stock_obj['dividend'].append(i['dividend'])
                data = pd.DataFrame(stock_obj)

                if cleaned:
                    return dropna(data)
                else:
                    return data
            else:
                return stock_obj
        except errors as err:
            print(f"ERROR: Could not select all stock data for symbol {ticker}")
            insert_error_log(f"ERROR: Could not select all stock data for symbol {ticker}")


    def update_stock_obj(self, stock_obj: stock_model):
        # check data
        result = True
        try:
            cursor = self.conn_stock.cursor()
            sql_statement = f"UPDATE STOCK_DATA " \
                            f"SET open='{stock_obj.open}', close='{stock_obj.close}', " \
                            f"high='{stock_obj.high}', low='{stock_obj.low}', " \
                            f"adj_close='{stock_obj.adj_close}', volume='{stock_obj.volume}', dividend='{stock_obj.dividend}'," \
                            f"split='{stock_obj.split}', sma_ind='{stock_obj.sma_ind}' " \
                            f"WHERE dt='{stock_obj.date}' AND ticker='{stock_obj.symbol}' ;"
            cursor.execute(sql_statement)
            self.conn_stock.commit()
        except errors:
            print(errors)
            insert_error_log(f"ERROR UPDATING TECHNICAL DATA INTO DATABASE FOR {stock_obj.symbol} AT {stock_obj.date}")


    def insert_stock_obj(self, stock_obj: stock_model):
        sql_statement = f"INSERT INTO STOCK_DATA (dt, ticker, open, close, high, low, adj_close, volume, dividend, split) " \
                                f"VALUES ('{stock_obj.date}', '{stock_obj.symbol}', {stock_obj.open}, {stock_obj.close}, {stock_obj.high}, " \
                                f"{stock_obj.low}, {stock_obj.adj_close}, {stock_obj.volume}, {stock_obj.dividend}, {stock_obj.split}) " \
                                f"ON DUPLICATE KEY UPDATE " \
                                f"open={stock_obj.open}, close={stock_obj.close}, high={stock_obj.high}, low={stock_obj.low}, " \
                                f"adj_close={stock_obj.adj_close}, volume={stock_obj.volume}, dividend={stock_obj.dividend}, split={stock_obj.split}"
        return sql_statement


    # Errors with adding a more detailed exist checker
    def check_if_exists(self, symbol, date, adj_close, volume, open, close, high, low, split, dividend):
        sql_statement = f"SELECT IF( EXISTS( SELECT * FROM STOCK_DATA WHERE dt = '{date}' AND volume = '{symbol}'), 1, 0);";
        try:
            cursor = self.conn_stock.cursor(buffered=True)
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except errors as error:
            insert_error_log(f"ERROR CHECKING TECHNICAL DATA FOR DATABASE FOR {symbol} AT {date}")
        if exists[0] == 1:
            return True
        else:
            return False

    def parse_stock_obj(self, ticker, data):
        if data != "Error":
            list = []
            try:
                for i in data:
                    stock_obj = stock_model.stock_model()
                    stock_obj.symbol = ticker
                    stock_obj.date = i['dt']
                    stock_obj.open = i['open']
                    stock_obj.close = i['close']
                    stock_obj.high = i['high']
                    stock_obj.low = i['low']
                    stock_obj.adj_close = i['adj_close']
                    stock_obj.volume = i['volume']
                    stock_obj.split = i['split']
                    stock_obj.dividend = i['dividend']
                    stock_obj.ad_ind = i['ad_ind']
                    stock_obj.adx_ind = i['adx_ind']
                    stock_obj.aroon_ind = i['aroon_ind']
                    stock_obj.bbands_ind = i['bbands_ind']
                    stock_obj.cci_ind = i['cci_ind']
                    stock_obj.ema_ind = i['ema_ind']
                    stock_obj.macd_ind = i['macd_ind']
                    stock_obj.obv_ind = i['obv_ind']
                    stock_obj.rsi_ind = i['rsi_ind']
                    stock_obj.sar_ind = i['sar_ind']
                    stock_obj.sma_ind = sma_model.sma.parse_from_database(i['sma_ind'])
                    stock_obj.stoch_ind = i['stoch_ind']
                    stock_obj.willr_ind = i['willr_ind']
                    list.append(stock_obj)
            except errors:
                print(f"Error: {errors}")
                insert_error_log("ERROR: Could not parse stock object during data load")
            return list
        else:
            return "Error"


    def to_dataframe(self, data):
        dict = {
            'dt' : [],
            'open' : [],
            'close' : [],
            'high' : [],
            'low' : [],
            'adj_close' : [],
            'volume' : [],
            'dividend' : [],
            'split' : []
        }
        for i, entry in enumerate(data):
            dict['dt'].append(entry.date)
            dict['open'].append(entry.open)
            dict['close'].append(entry.close)
            dict['high'].append(entry.high)
            dict['low'].append(entry.low)
            dict['adj_close'].append(entry.adj_close)
            dict['volume'].append(entry.volume)
            dict['dividend'].append(entry.dividend)
            dict['split'].append(entry.split)
        return pd.DataFrame(dict)
