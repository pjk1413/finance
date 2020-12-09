import Data.Technical_Data.get_data as data
import Data.Technical_Data.Model.stock_model as stock
import Data.Technical_Data.Model.stock_model as stock
import Database.build_database as database
import Database.build_tables as tables
import Init.init as init
from Init import init
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Technical_Data.Retrieve_Data.retrieve_indicator as ri
import Data.Technical_Data.Retrieve_Data.retrieve_sentiment as rs

import Database.Service.sentiment_service as ss

if __name__ == '__main__':

    print(ss.sentiment_service().check_if_exists("", "", "", "", ""))

    # ri.retrieve_indicator_data().run_data_load()

    # dict = ri.retrieve_indicator_data().psar_ind('GM')

    # rt.retrieve_technical_data()

    # tables.build_tables().create_reference_table()
    # data = rs.retrieve_sentiment_data().retrieve_historical('GM')
    # print(data)
    # rt.retrieve_technical_data().run_data_load('latest')
    # init().startup()
    # yfinance.yfinance().update_data()
    # web_crawler.web_crawler("https://www.marketwatch.com/investing/stock/goog").start()
    # data.retrieve_data().pratice()






