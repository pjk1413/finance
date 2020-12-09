import datetime
import StockAPI.Robinhood_API as Robinhood
import StockAPI.YFinance_API as yfinance
import StockAPI.Finnhub_API as finnhub
import StockAPI.Alphavantage_API as alphaV
import StockAPI.Morningstar_API as morning_star
import StockAPI.Zirra_API as zirra_api

from DatabaseManagement_DEL.MySQLData import mysql_db

# All Dates are in UTC/UNIX format

class getStockPrices:
    def __init__(self, symbol, market):
        self.symbol = symbol
        self.market = market
        self.current_day = datetime.datetime.utcnow()
        self.date = None
        self.date_range = []
        self.sql = mysql_db()

    @staticmethod
    def get_ticker_symbol_list():
        sql_statement = "SELECT * FROM ticker_symbols;"
        sql = mysql_db()

        cursor = sql.conn_finance.cursor()
        cursor.execute(sql_statement)
        symbol_list = cursor.fetchall()
        return symbol_list

    def insert_symbol_data_candlestick_1D(self):
        """
        Grabs and cleans stock data from various sources, returns cleaned data.

        --- Doesn't currently have the ability to check if a symbol needs to be updated
        :return:
        """
        data = None

        # table status will be a number that checks if the table exists, if it has been updated recently (0 - nothing needed, 1 - standard update, 2 - nothing present)
        # table_status = self.get_table_status("candlestick_1D")

        # Convert this to run in a loop to easily insert new api's in the future
        yf = yfinance.yfinance_api(self.symbol)
        try:
            # Date Range can be 'max' or a tuple of two dates
            self.date_range = yf.get_date_range()
            data = yf.get_stock_data(self.date_range)
            if data != -1:
                result = self.sql.insert_data_candlestick(data)
        except:
            print("Error inserting Yahoo Finance Data")
            self.sql.log_error(f"Error inserting/finding data for symbol {self.symbol} using Yahoo Finance for dates in range {self.date_range[0]} - {self.date_range[1]}")


            robin = Robinhood.robinhood_api(self.symbol)
            try:
                self.date_range = robin.get_date_range()
                data = robin.get_stock_data(self.date_range)
                if data != -1:
                    result = self.sql.insert_data(data)
            except:
                self.sql.log_error(
                    f"Error inserting/finding data for symbol {self.symbol} using Robinhood for dates in range {self.date_range[0]} - {self.date_range[1]}")


                alpha = alphaV.alphavantage_api(self.symbol)
                try:
                    self.date_range = alpha.get_date_range()
                    data = alpha.get_stock_data(self.date_range)
                    if data != -1:
                        result = self.sql.insert_data(data)
                except:
                    self.sql.log_error(
                        f"Error inserting/finding data for symbol {self.symbol} using AlphaVantage for dates in range {self.date_range[0]} - {self.date_range[1]}")


                    # zirra = zirra_api.zirra_api(self.symbol)
                    # try:
                    #     self.date_range = zirra.get_date_range()
                    #     data = zirra.get_stock_data(self.date_range)
                    #     if data != -1:
                    #         self.sql.insert_data(data)
                    # except:
                    #     self.sql.log_error(
                    #         f"Error inserting/finding data for symbol {self.symbol} using Zirra for dates in range {self.date_range[0]} - {self.date_range[1]}")


    def get_table_status(self, table_name):
        """
        Returns the table status and a date range if possible
        :return:
        """
        # Get rows and check dates
        rows = self.sql.get_table_rows(table_name)
        if rows == 0:
            return 2
        elif rows > 0:
            date_list = self.sql.get_dates_until_now(self.symbol)
            return 1
        else:
            # Check that date range
            return 0


# Needs a number of sources to check current list against, adds in ticker symbols not present
class ticker_symbol_update:
    def __init__(self):
        pass


# use this class to clean/update the data periodically
class clean_update_data:
    def __init__(self):
        pass