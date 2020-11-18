import yfinance as yf
import re
import datetime
from Database.database import database
import mysql.connector as connect
import pandas as pd
import math
from Database.crud_func import crud
import Interface.utility as utility

class yfinance:
    def __init__(self):
        self.conn = database().conn_finance
        self.market = None
        self.date_range = None
        self.validate = False
        self.stock_table_list_name = "STOCK_LIST_TBL"
        self.tickers = crud().get_list_of_stocks()


    def validate_symbol(self, symbol):
        if re.match("[a-zA-z]{1,8}", symbol): # Ensure that symbol matches basic pattern of symbols
            symbol_data = self.conn.get_symbol(symbol) # Search database for that symbol, if exists, return symbol and market
            if symbol_data['Status']: # returns true if symbol is found in database
                self.market = symbol_data['Market']
                return symbol
            else:
                # self.sql.logError(f"Symbol not found in database or symbol format error on: '{symbol}'")
                return None


    def get_date_range(self):
        sql_statement = f"SELECT dt FROM {self.symbol}_TBL ORDER BY dt DESC LIMIT 1;"
        recent_date = None

        try:
            cursor = self.sql.conn_finance.cursor()
            cursor.execute(sql_statement)
            recent_date = cursor.fetchone()
        except:
            database().insert_error_log(f"ERROR GETTING DATE RANGE FOR YFINANCE {self.symbol}")

        current_date = datetime.datetime.now()
          # Grab most recent date on the table and compare with current date
        if recent_date == None:
            return 'max'
        else:
            difference = current_date - recent_date[0]
            if difference.days > 99:
                return 'max'
            else:
                return (recent_date[0], current_date)


    def validate_date_range(self):
        if self.date_range == 'max':
            return False

        for date in self.date_range:
            if not re.match("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}", date):
                print("yfinance Date Format Error - Date must be astring in the YYYY-MM-DD format or 'max'")
                self.sql.logError(f"Date range given is not valid for: '{self.date_range}'")
                return False
        return True


    def update_data(self):
        period_dict = self.get_period()
        for period in period_dict:
            print(f"{period}: {len(period_dict[period])}")
        l = len(period_dict)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for x, period in enumerate(period_dict):
            ticker_list = ""

            if len(period_dict[period]) == 0:
                continue

            print(f"Beginning download for {period}")
            for ticker in period_dict[period]:
                ticker_list += ticker + " "

            try:
                data = yf.download(tickers=ticker_list, threads=False, group_by="ticker", period=period)
            except:
                database().insert_error_log("YFINANCE IS DOWN OR NOT WORKING")
                utility.restart_yfinance()
                return

            for symbol in period_dict[period]:

                stock_obj = None
                try:
                    stock_obj = data.get(symbol, None)
                    if stock_obj == None:
                        continue
                except:
                    database().insert_error_log(f"ERROR FINDING TECHNICAL DATA FOR {symbol}")
                    continue
                for header in stock_obj.columns.values:

                    for i in range(0, len(stock_obj[header].index)):
                        if self.check_values(stock_obj, i):
                            dt = self.frmt_dt(stock_obj[header].index[i])
                            date = dt.strftime("%Y-%m-%d %H:%M:%S")
                            open = self.flt_num(stock_obj['Open'][i])
                            close = self.flt_num(stock_obj['Close'][i])
                            high = self.flt_num(stock_obj['High'][i])
                            low = self.flt_num(stock_obj['Low'][i])
                            adj_close = self.flt_num(stock_obj['Adj Close'][i])
                            volume = self.int_num(stock_obj['Volume'][i])

                            values = self.check_insert_values((date, open, close, high, low, adj_close, volume))

                            if values == False:
                                continue


                            try:
                                cursor = self.conn.cursor()
                                result = self.check_if_exists(symbol, date, adj_close, volume, open, close)
                                if result:
                                    sql_statement = f"UPDATE {symbol}_STK " \
                                                    f"SET dt='{date}', open='{open}', close='{close}', high='{high}', low='{low}', " \
                                                    f"adj_close='{adj_close}', volume='{volume}' " \
                                                    f"WHERE ID='{result}';"
                                    cursor.execute(sql_statement)
                                    self.conn.commit()
                                elif result == False:
                                    sql_statement = f"INSERT INTO {symbol}_STK (dt, open, close, high, low, adj_close, volume) " \
                                                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                                    cursor.execute(sql_statement, values)
                                    self.conn.commit()
                            except connect.errors as err:
                                database().insert_error_log(f"ERROR INSERTING/UPDATING TECHNICAL DATA INTO DATABASE FOR {symbol} AT {date}")
        utility.printProgressBar(x + 1, l, prefix='Progress:', suffix='Complete', length=50)


    def check_if_exists(self, symbol, date, adj_close, volume, open, close):
        # returns 1 if exists, 0 if not
        sql_statement = f"SELECT IF( EXISTS( SELECT * FROM {symbol}_STK WHERE dt = '{date}'), 1, 0)";


        data = None
        try:
            cursor = self.conn.cursor(buffered=True)
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except connect.errors as error:
            database().insert_error_log(f"ERROR CHECKING TECHNICAL DATA FOR DATABASE FOR {symbol} AT {date}")

        if exists[0] == 1:
            return True
        else:
            return False


    def check_insert_values(self, values):
        count = 0
        for value in values:
            if value == None or value == "":
                count += 1

        if count > 0:
            return False
        else:
            return values


    def check_values(self, stock_obj, i):
        if math.isnan(stock_obj['Open'][i]) or math.isnan(stock_obj['Close'][i]) or math.isnan(stock_obj['High'][i]) or math.isnan(stock_obj['Low'][i]):
            return False
        else:
            return True


    def get_period(self):
        sql_statement = f"SELECT ticker FROM finance_quant.{self.stock_table_list_name};"

        period_dict = {
            "1d" : [],
            "5d" : [],
            "1mo" : [],
            "3mo" : [],
            "6mo" : [],
            "1y" : []
        }

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            temp_data = cursor.fetchall()
        except connect.errors as err:
            database().insert_error_log(f"ERROR FETCHING ALL SYMBOLS FOR PERIOD LENGTH SEARCH")
        print("Finding Ideal Period Length for each Symbol...")

        l = len(temp_data)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for z, ticker in enumerate(temp_data):
            # ticker = tick[0]
            if ticker[0] == "ZXYZ.A":
                continue
            sql_statement = f"SELECT dt FROM {ticker[0]}_STK ORDER BY dt DESC LIMIT 1;"
            recent_date = None
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql_statement)
                recent_date = cursor.fetchone()
            except connect.errors as error:
                database().insert_error_log(f"ERROR FINDING IDEAL PERIOD FOR TECHNICAL DATA INTO DATABASE FOR {ticker[0]}")

            current_date = datetime.datetime.now()

            if recent_date == None or recent_date == "":
                period_dict["1y"].append(ticker[0])
            else:
                difference = current_date - recent_date[0]
                if difference.days < 1:
                    continue
                elif difference.days < 2:
                    period_dict["1d"].append(ticker[0])
                elif difference.days < 6:
                    period_dict["5d"].append(ticker[0])
                elif difference.days <32:
                    period_dict["1mo"].append(ticker[0])
                elif difference.days < 94:
                    period_dict["3mo"].append(ticker[0])
                elif difference.days < 190:
                    period_dict["6mo"].append(ticker[0])
                elif difference.days < 366:
                    period_dict["1y"].append(ticker[0])

            utility.printProgressBar(z + 1, l, prefix='Progress:', suffix='Complete', length=50)
        return period_dict


    def frmt_dt(self, date):
        if date == 'nan':
            return 'NAN'
        else:
            return date


    def int_num(self, number):
        if math.isnan(number):
            return 0000
        else:
            return int(number)


    def flt_num(self, number):
        if math.isnan(number):
            return float('NaN')
        else:
            return round(float(number),3)

