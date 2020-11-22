import Data.Crawler.web_crawler as web_crawler
import Data.Technical_Data.yahoo_finance as yfinance
import Data.Technical_Data.get_data as data
import Data.Technical_Data.Stock_Utility.stock_model as stock

if __name__ == '__main__':
    stock.stock_daily()
    # init().startup()
    # yfinance.yfinance().update_data()
    # web_crawler.web_crawler("https://www.marketwatch.com/investing/stock/goog").start()
    data.retrieve_data().pratice()






