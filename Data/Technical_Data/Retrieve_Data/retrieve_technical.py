from Data.config_read import config as con
import Data.Technical_Data.Model.stock_model as stock_model
from Data.Technical_Data.Model.stock_model import stock_model
import requests
from Data.stock_list import stock_list
from Database.database import insert_error_log
import datetime
import json
import requests.exceptions as exc
from Database.Service.stock_service import stock_service as service
import multiprocessing as mp
from Data.Technical_Data.Stock_Utility.date_helper import find_most_recent_date
from Utility.string_manipulation import clean
from Interface.utility import printProgressBar

class retrieve_technical_data:
    def __init__(self):
        config = con()
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()


    def run_data_load(self, range = 'latest'):
        processList = []
        list_of_processes = []
        l = len(self.list_of_stocks)
        try:
            if range == 'historical':
                for stock in self.list_of_stocks:
                    processList.append(mp.Process(target=self.parse_stock_obj, args=(stock[0], self.retrieve_historical(stock[0]))))

            if range == 'latest':
                for i, stock in enumerate(self.list_of_stocks):
                    processList.append(mp.Process(target=self.parse_stock_obj, args=(stock[0], self.retrieve_latest(stock[0]))))

            printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
            for i, x in enumerate(processList):
                x.start()
                list_of_processes.append(x)
                if len(list_of_processes) > self.np:
                    for y in list_of_processes:
                        y.join()
                printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            # Cleanup
            for x in processList:
                x.join()
        except:
            insert_error_log("ERROR: Multiprocessor threw an error during data load")


    def retrieve_historical(self, ticker, years_back = 2):
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        if start_date is not None:
            response = requests.get(f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
                                    f"?startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
            if "Error:" in response.text:
                insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
                return "Error"
            else:
                return json.loads(response.text)
        else:
            return "Error"


    def retrieve_latest(self, ticker, years_back = 2):
        start_date = find_most_recent_date(ticker)
        if start_date is None:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        else:
            start_date = start_date[0].date()
        response = requests.get(f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
        if "Error:" in response.text:
            insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
            return "Error"
        else:
            return json.loads(response.text)


    def parse_stock_obj(self, ticker, data):
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
                    service().insert_stock_obj(stock_obj)
            except exc:
                insert_error_log("ERROR: Could not parse stock object during data load")
                print(exc)
        else:
            return