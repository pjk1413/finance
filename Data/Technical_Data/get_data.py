# https://api.tiingo.com/products/end-of-day-stock-price-data
# https://www.alphavantage.co/premium/ - free api key: KHWQTGUQ4KFEIFBM
# https://www.quandl.com/tools/api
# https://eodhistoricaldata.com/
# https://iexcloud.io/core-data/

import requests
import Data.config_read as get_values
import json


class retrieve_data:
    def __init__(self):
        config = get_values.config()
        self.alpha_vantage_api_key = config.alpha_vantage_api_key
        pass

    def retrieve_stock_data(self):
        # have a background data function that puts together data
        # make decisions about the top 1000 stocks to be watching on a weekly basis
        # those top 1000 stocks will be watched closely
        # daily
        # All stock data - 1
        # All technical data - 53
        # weekly
        # All fundamental data - 4 (balance sheets etc...)

        pass

    def pratice(self):
        response = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey={self.alpha_vantage_api_key}")
        print(response.status_code)
        # print(response.content)
        # print(response.text)
        data = json.loads(response.text)
        x = data["Time Series (Daily)"]["2020-11-20"]

        print(x)

    def income_statement(self):
        response = requests.get(f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=IBM&apikey={self.alpha_vantage_api_key}")
        print(response.text)