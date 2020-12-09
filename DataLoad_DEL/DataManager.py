from DatabaseManagement_DEL.MySQLData import mysql_db
import datetime


class data_manager:
    def __init__(self):
        self.symbol = None
        self.market = None
        self.current_day = datetime.datetime.utcnow()
        self.date = None
        self.date_range = []
        self.sql = mysql_db()
        self.symbol_list = self.ticker_symbol_list()

    def ticker_symbol_list(self):
        sql_statement = "SELECT * FROM ticker_symbols;"
        sql = mysql_db()

        cursor = sql.conn_finance.cursor()
        cursor.execute(sql_statement)
        symbol_list = cursor.fetchall()
        return symbol_list

    def insert_candlestick_30m(self):
        pass

    def insert_candlestick_1D(self):
        pass
