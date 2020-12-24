import yfinance as yf
import re
from DatabaseManagement_DEL.MySQLData import mysql_db
import time
import datetime

class yfinance_api:
    def __init__(self, symbol):
        self.sql = mysql_db()
        self.market = None
        self.symbol = self.validate_symbol(symbol)
        self.date_range = None
        self.validate = False



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

        if(data.empty):
            self.validate = False
            return

        for i in range(0, row_count):
            row = data.iloc[i, :]
            for x in row:
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
        cleaned_data = []
        count_rows = data.shape[0]

        for i in range(0, count_rows):
            row = data.iloc[i, :]

            open = row.loc['Open']
            close = row.loc['Close']
            high = row.loc['High']
            low = row.loc['Low']
            volume = row.loc['Volume']
            date = row.name

            row_dict = {
                'Open' : round(float(open),2),
                'Close' : round(float(close),2),
                'High' : round(float(high),2),
                'Low' : round(float(low),2),
                'Volume' : round(float(volume),2),
                'Date' : date.strftime("%Y-%m-%d %H:%M:%S"), # Is a timestamp right now
                'Market' : self.market,
                'Symbol' : self.symbol
            }

            cleaned_data.append(row_dict)

        return cleaned_data


    def validate_date_range(self):
        if self.date_range == 'max':
            return False

        for date in self.date_range:
            if not re.match("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}", date):
                print("yfinance Date Format Error - Date must be astring in the YYYY-MM-DD format or 'max'")
                self.sql.logError(f"Date range given is not valid for: '{self.date_range}'")
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


    def get_date_range(self):
        sql_statement = f"SELECT dt FROM candlestick_1D WHERE symbol='{self.symbol}' ORDER BY dt DESC LIMIT 1;"

        cursor = self.sql.conn_finance.cursor()
        cursor.execute(sql_statement)

        current_date = datetime.datetime.now()
        recent_date = cursor.fetchone()  # Grab most recent date on the table and compare with current date
        if recent_date == None:
            return 'max'
        else:
            difference = current_date - recent_date[0]
            if difference.days > 99:
                return 'max'
            else:
                return (recent_date[0], current_date)


    def get_stock_data(self, date_range):
        '''
        Function to get stock data from the yfinance api
        :param date_range: can be a string 'max' for all data, or a tuple date_range start(0), end(1)
        start/end must be formatted as 'YYYY-MM-DD' as a string
        Data does NOT include the end date
        :return: returns a pandas datatable of all available data
        '''

        if self.symbol == None:
            print("Symbol not found in database")
            self.sql.log_error("Symbol not found in database or error in format")
            return -1

        symbol_data = yf.Ticker(self.symbol)
        if date_range == 'max':

            data = symbol_data.history(period=date_range, interval='1D', actions=False)
            self.validate_data(data)
            if(self.validate):
                return self.clean_data(data)
            else:
                print("Error with yfinance data")
                self.sql.logError(f"Error with yfinance data for symbol {self.symbol}")
                return -1
        elif len(date_range) > 1:
            if self.validate_date_range():
                data = symbol_data.history(start=date_range[0], end=date_range[1], interval='1D', actions=False, prepost=True)
                self.validate_data(data)
                if(self.validate):
                    return self.clean_data(data)
        else:
            print(f"No valid date range given for {self.symbol} for yfinance")
            self.sql.log_error(f"No valid date range given for {self.symbol} between {date_range[0]} and {date_range[1]}")
            return -1