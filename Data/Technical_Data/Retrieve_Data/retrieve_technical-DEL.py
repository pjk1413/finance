from json import JSONDecodeError
from config_read import config as con
import Data.Technical_Data.Model.stock_model as stock_model
from Data.Technical_Data.Model.stock_model import stock_model
import requests
import Utility.multithreading as multi_threading
from Data.stock_list import stock_list
import time
from Database.Service.database import insert_error_log
import datetime
import json
import requests.exceptions as exc
from Utility.string_manipulation import clean
from Interface.utility import printProgressBar

class retrieve_technical_data:
    def __init__(self):
        config = con()
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()

    def run_data_load(self, range = 'latest'):
        start = time.perf_counter()
        l = len(self.list_of_stocks)
        printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for i, entry in enumerate(self.list_of_stocks):
            stock_obj_list = self.parse_stock_obj(entry[0], self.retrieve_data(entry[0], range))
            sql_statement_list = []
            for obj in stock_obj_list:
                sql_statement_list.append(self.insert_stock_obj(obj))
            threader = multi_threading.Multi_Threading(sql_statement_list, "technical")
            threader.run_insert()
            printProgressBar(i + 1, l, prefix=f'Current: {entry[0]} - Progress:', suffix='Complete', length=50)
        finish = time.perf_counter()
        print(f"Downloaded stock technical data in {finish - start} seconds \n")

    # TODO Needs error handling based on response
    def retrieve_data(self, ticker, range, years_back = 2):
        if range == "historical":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        elif range == "latest":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        elif range == "month":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        if start_date is not None:
            try:
                response = requests.get(f"https://api.tiingo.com/tiingo/daily/{ticker.replace('_','-')}/prices"
                                    f"?startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
                if "Error:" in response.text:
                    insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
                    return "Error"
                else:
                    try:
                        return json.loads(response.text)
                    except JSONDecodeError:
                        print("ERROR")
            except requests.exceptions.RequestException:
                print("ERROR")
                return "Error"
        else:
            return "Error"

    # TODO be sure to call the correct exception
    def parse_stock_obj(self, ticker, data):
        obj_list = []
        if data != "Error":
            try:
                for stock_json in data:
                    stock_obj = stock_model()
                    stock_obj.date = datetime.datetime.strptime(stock_json['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    stock_obj.open = stock_json['open']
                    stock_obj.close = stock_json['close']
                    stock_obj.high = stock_json['high']
                    stock_obj.low = stock_json['low']
                    stock_obj.adj_close = stock_json['adjClose']
                    stock_obj.volume = stock_json['volume']
                    stock_obj.symbol = ticker
                    stock_obj.split = stock_json['splitFactor']
                    stock_obj.dividend = stock_json['divCash']
                    obj_list.append(stock_obj)
                return obj_list
            except exc:
                insert_error_log("ERROR: Could not parse stock object during data load")
                print(exc)
        else:
            return

    def insert_stock_obj(self, stock_obj: stock_model):
        sql_statement = f"INSERT INTO STOCK_DATA (dt, ticker, open, close, high, low, adj_close, volume, dividend, split) " \
                                f"VALUES ('{stock_obj.date}', '{stock_obj.symbol}', {stock_obj.open}, {stock_obj.close}, {stock_obj.high}, " \
                                f"{stock_obj.low}, {stock_obj.adj_close}, {stock_obj.volume}, {stock_obj.dividend}, {stock_obj.split}) " \
                                f"ON DUPLICATE KEY UPDATE " \
                                f"open={stock_obj.open}, close={stock_obj.close}, high={stock_obj.high}, low={stock_obj.low}, " \
                                f"adj_close={stock_obj.adj_close}, volume={stock_obj.volume}, dividend={stock_obj.dividend}, split={stock_obj.split}"
        return sql_statement