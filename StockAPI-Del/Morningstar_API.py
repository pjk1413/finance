
from DatabaseManagement_DEL.MySQLData import mysql_db
import re
import datetime

class morningstar_api:
    def __init__(self, symbol):
        self.sql = mysql_db()
        self.market = None
        self.symbol = self.validate_symbol(symbol)
        self.date_range = None
        self.validate = False
        self.otp_code = pyotp.TOTP("ISADDZE3WCOKL2JG").now()
        self.login = robin_stocks.authentication.login('pjk1413@gmail.com', '394508Google!', self.otp_code)

    # def test(self):
    #     data = robin_stocks.stocks.get_stock_historicals('A', interval='day', span='week')
    #     print(data)

    def get_date_range(self, date_range):
        pass

    #     returns a date range based on the dates given, convert to best possible range

    def validate_symbol(self, symbol):

        if re.match("[a-zA-z]{1,8}", symbol):  # Ensure that symbol matches basic pattern of symbols
            symbol_data = self.sql.get_symbol(
                symbol)  # Search database for that symbol, if exists, return symbol and market
            if symbol_data['Status']:  # returns true if symbol is found in database
                self.market = symbol_data['Market']
                return symbol
            else:
                self.sql.log_error(f"Symbol not found in database or symbol format error on: '{symbol}'")
                return None

    def validate_data(self, data):
        """
        Validates data to be used when saving to database, checks for nulls and zero values
        :param data:
        :return:
        """

        if (len(data) == 0):
            self.validate = False
            return

        for row in data:

            if row['open_price'] == 0 or row['close_price'] == 0 or row['high_price'] == 0 or row['low_price'] == 0 or \
                    row['volume'] == 0:
                self.validate = False
                return

        self.validate = True

    def clean_data(self, data):
        """
        cleans data to be inserted into sql statement
        :param data: pandas data table from yfinance
        :return: dictionary of value pairs
        """
        cleaned_data = []

        for row in data:
            open = row['open_price']
            close = row['close_price']
            high = row['high_price']
            low = row['low_price']
            volume = row['volume']
            date = datetime.datetime.strftime(datetime.datetime.strptime(row['begins_at'], "%Y-%m-%dT%H:%M:%SZ"),
                                              "%Y.%m.%d:%H.%M.%S")

            row_dict = {
                'Open': float(open),
                'Close': float(close),
                'High': float(high),
                'Low': float(low),
                'Volume': float(volume),
                'Date': date,  # Is a timestamp right now
                'Market': self.market,
                'Symbol': self.symbol
            }

            cleaned_data.append(row_dict)

        return cleaned_data

    def validate_date_range(self):
        """
        Need to validate the date range in a different manner for the robinhood_api
        :return:
        """
        if self.date_range == 'max':
            return False

        for date in self.date_range:
            if not re.match("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}", date):
                print("Robinhood Date Format Error - Date must be astring in the YYYY-MM-DD format or 'max'")
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

    def get_date_range(self):
        sql_statement = f"SELECT dt FROM candlestick_1D WHERE symbol='{self.symbol}' ORDER BY dt DESC LIMIT 1;"

        cursor = self.sql.conn_finance.cursor()
        cursor.execute(sql_statement)

        current_date = datetime.datetime.now()
        recent_date = cursor.fetchone()  # Grab most recent date on the table and compare with current date
        difference = current_date - recent_date[0]

        if recent_date == None:
            return '5year'
        else:
            if difference.days == 1:
                return 'day'
            elif difference.days > 1 and difference.days < 8:
                return 'week'
            elif difference.days > 7 and difference.days < 28:
                return 'month'
            elif difference.days > 27 and difference.days < 90:
                return '3month'
            elif difference.days > 89 and difference.days < 365:
                return 'year'
            else:
                return '5year'

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
        self.span = self.get_date_range(self.date_range)
        if date_range == '5year' or date_range == 'year' or date_range == '3month' or date_range == 'month' or date_range == 'week' or date_range == 'day':
            data = robin_stocks.stocks.get_stock_historicals(self.symbol, interval='day', span=self.span)
            self.validate_data(data)
            if (self.validate):
                return self.clean_data(data)
            else:
                print("Error with robinhood data")  # This needs to be looked at closer, may need to be returned
                self.sql.log_error("Error with robinhood data")
                return
        else:
            return "Error: No valid date range given"