import yfinance as yf
from Init.init import init as init
from Database.crud_func import crud
import Database.build_database as db
import Database.build_tables as build
from Database.database import database
import Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Interface.Interface as interface


if __name__ == '__main__':

    # db.build_database().build_database()
    # build.build_tables().build_tables()
    init().startup()

    # interface.view().start_up()

    # stk = yfinance.yfinance()
    # stk.update_data()
    # seni = sentiment.sentiment()
    # seni.gather_headlines()


def add_columns():
    stock_list = crud().get_list_of_stocks()
    conn = database().conn_finance
    for stock in stock_list:
        sql_statement = f"""ALTER TABLE {stock}_STK 
                        ADD COLUMN sentiment VARCHAR(50);"""
        cursor = conn.cursor()
        cursor.execute(sql_statement)


    # init().first_start()
    # data = yf.download(tickers="AAPL GOOG SPY MSFT", period="1d", group_by="ticker")
    # for x in data:
    #     print(f"{x} : {data[x]}")
    # # print(data)







