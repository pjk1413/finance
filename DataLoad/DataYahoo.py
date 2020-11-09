import yfinance as yf
from DatabaseManagement.Connection import connect_mysql as mysql
# https://aroussi.com/post/python-yahoo-finance


class candlestick:
    def __init__(self, host, username, password):
        self.conn = mysql(host, username, password)
        pass

    @staticmethod
    def get_candlestick_data_single(self, symbol):
        conn = mysql("192.168.1.57", "patrick", "12345")
        conn.connect_to_db("finance_quant")
        sql_statement = f"SELECT high, low, open, close, ticker FROM finance_quant.candlestick_1D WHERE ticker='{symbol}';"
        data = conn.execute_sql_statement_select(sql_statement)
        print(float(data[0][0]))



    @staticmethod
    def get_historical_data_hour(ticker):
        data = yf.Ticker(ticker)
        return data.history(period="2y", interval="60m")

    @staticmethod
    def get_historical_data_10y(ticker):
        data = yf.Ticker(ticker)
        return data.history(period="10y", interval="1D")

class y_finance:
    def __init__(self, ticker):
        self.ticker = ticker
        self.data = yf.Ticker(ticker)

    def getHistoricalDataWeek(self):
        pass

    def getHistoricalDataDay(self):
        stock_history = self.data.history(period="max", interval="1d")

    def getHistoricalDataHour(self):

        #Check if table exists

        #Period must be within the last 730 days
        stock_history = self.data.history(period="2y", interval="60m")

        count_row = stock_history.shape[0]
        print(f"------- {self.ticker} ------- Rows: {count_row}")
        for x in range(0, count_row):
            row = stock_history.iloc[x, :]
            high = row.loc['High']
            low = row.loc['Low']
            open = row.loc['Open']
            close = row.loc['Close']
            volume = row.loc['Volume']
            dividends = row.loc['Dividends']
            stock_splits = row.loc['Stock Splits']

            #open high low close volume dividends stocksplits date
            # print(row.loc['High'])
            # print(row.loc['Low'])


