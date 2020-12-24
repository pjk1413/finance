import finnhub
from DatabaseManagement_DEL.MySQLData import mysql_db
import re
import pandas as pd
import datetime
import time
from datetime import date
import requests

class finnhub_api:
    def __init__(self, symbol):
        self.client = finnhub.Client("btclgpv48v6rudshgts0")
        self.sql = mysql_db()
        self.market = None
        self.symbol = self.validate_symbol(symbol)
        self.date_range = None
        self.validate = False


    # def test(self, date_range):
    #     data = self.client.stock_candles(self.symbol, resolution='D', _from=date_range[0], to=date_range[1])
    #     new_data = pd.DataFrame(data)


    def validate_symbol(self, symbol):

        if re.match("[a-zA-z]{1,8}", symbol): # Ensure that symbol matches basic pattern of symbols
            symbol_data = self.sql.get_symbol(symbol) # Search database for that symbol, if exists, return symbol and market
            if symbol_data['Status']: # returns true if symbol is found in database
                self.market = symbol_data['Market']
                return symbol
            else:
                # self.sql.logError(f"Symbol not found in database or symbol format error on: '{symbol}'")
                return None

    def validate_data(self, data):
        row_count = data.shape[0]

        if (data.empty):
            self.validate = False
            return

        for i in range(0, row_count):
            row = data.iloc[i, :]
            for x in row:
                if x != 'ok':
                    if x > 0:
                        continue
                    elif x == None or x == 0:
                        self.validate = False
                        # sql.logError(f"Data is either null or 0 on: '{x}'")
                        return
        self.validate = True

    def clean_data(self, data):
        """
        cleans data to be inserted into sql statement
        :param data: pandas data table from yfinance
        :return: dictionary of value pairs
        """
        try:
            cleaned_data = []
            count_rows = data.shape[0]

            for i in range(0, count_rows):
                row = data.iloc[i, :]

                open = round(row.loc['o'] , 2)
                close = row.loc['c']
                high = row.loc['h']
                low = row.loc['l']
                volume = row.loc['v']
                date = datetime.datetime.fromtimestamp(row.loc['t'])
                row_dict = {
                    'Open': open,
                    'Close': float(close),
                    'High': float(high),
                    'Low': float(low),
                    'Volume': float(volume),
                    'Date': date.strftime("%Y.%m.%d:%H.%M.%S"),  # Is a timestamp right now
                    'Market': self.market,
                    'Symbol': self.symbol
                }

                cleaned_data.append(row_dict)
        except:
            print("ERROR")
            self.sql.log_error(f"Error cleaning data in finnhub for {self.symbol} at {datetime.datetime.now()}")
        return cleaned_data


    def clean_date_range(self):
        """
        Returns a unix date value after taking in a string in format "YYYY-MM-DD"
        :return:
        """
        new_date_range = []

        for dt in self.date_range:

            year = int(dt.split("-")[0])
            month = int(dt.split("-")[1])
            day = int(dt.split("-")[2])

            dateTime = datetime.date(year, month, day)

            unix_time = time.mktime(dateTime.timetuple())
            new_date_range.append(unix_time)
            


        self.date_range = new_date_range


    def validate_date_range(self):
        """
        Need to validate the date range in a different manner for the robinhood_api
        :return:
        """
        if self.date_range == 'max':
            return False

        for date in self.date_range:
            if not re.match("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}", date):
                print("Finnhub Date Format Error - Date must be a string in the YYYY-MM-DD")
                self.sql.log_error(f"Date range given is not valid for: '{self.date_range}'")
                return False
        return True

    def get_stock_data_single(self):
        """
        Not currently used, will be needed if we decide to move towards multi symbol pull downs
        :return:
        """
        pass

    def get_stock_data_multi(self):
        """
        Not currently used, dead function, will be used if we decide to do multi symbol data grabs in the future
        """
        pass

    def get_stock_data(self, date_range):
        '''
        Function to get stock data from the yfinance api
        :param date_range: can be a string 'max' for all data, or a tuple date_range start(0), end(1)
        start/end must be formatted as 'YYYY-MM-DD' as a string
        Data does NOT include the end date
        :return: returns a pandas datatable of all available data
        '''
        if self.symbol == None:
            print("Symbol not found in database or error in format")
            return

        self.date_range = date_range

        if len(date_range) > 1:
            if self.validate_date_range():
                self.clean_date_range()
                print(date_range)

                r = requests.post('https://finnhub.io/api/v1/webhook/add?token=btclgpv48v6rudshgts0', json={'event': 'earnings', 'symbol': 'AAPL'})
                res = r.json()
                print(res)

                data = self.client.stock_candles(self.symbol, resolution='D', _from=self.date_range[0], to=self.date_range[1])

                data = pd.DataFrame(data)
                self.validate_data(data)
                if (self.validate):
                    return self.clean_data(data)

        else:
            return "Error: No valid date range given"