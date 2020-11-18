from Database.database import database
import yfinance as yf
from Data.config_read import config as get_values


class crud:
    def __init__(self):
        config = get_values()
        self.conn = database().conn_finance
        self.stock_table_list_name = "STOCK_LIST_TBL"



    def check_stock_entry_in_list(self, ticker):
        sql_statement = f"SELECT ticker, sector, industry FROM STOCK_LIST_TBL WHERE ticker='{ticker}';"
        cursor = self.conn.cursor()

        try:
            cursor.execute(sql_statement)
            entry = cursor.fetchone()

            if entry[0].strip() == ticker.strip():
                if entry[1] == None or entry[1] == "" or entry[2] == None or entry[2] == "":
                    symbol = yf.Ticker(ticker)
                    sector, industry = None, None
                    try:
                        sql_statement = f"INSERT INTO {self.stock_table_list_name} (sector, industry) WHERE ticker={ticker} " \
                        f"VALUES (%s, %s);"
                        values = (symbol.info["sector"], symbol.info["industry"])
                        cursor.execute(sql_statement, values)
                    except:
                        pass
                return True
            else:
                return False
        except:
            return False


    def get_list_of_stocks(self):
        sql_statement = f"SELECT ticker FROM STOCK_LIST_TBL;"

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn.get_warnings)
            print("ERROR FETCHING STOCK_LIST_TBL")

