import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from Data.config_read import config as con
from Database.database import database
from Utility.string_manipulation import clean
from Interface.utility import printProgressBar
import multiprocessing as mp
from Database.database import insert_error_log
import datetime
import requests
import json

class retrieve_sentiment_data:
    def __init__(self):
        config = con()
        self.conn_stock = database().conn_stock
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        # self.list_of_stocks = stock_list().get_list_of_stocks()
        self.list_of_stocks = [('GM',)]

    def run_data_load(self, range='latest'):
        processList = []
        list_of_processes = []
        l = len(self.list_of_stocks)
        try:
            if range == 'historical':
                for stock in self.list_of_stocks:
                    processList.append(
                        mp.Process(target=self.parse_stock_obj, args=(stock[0], self.retrieve_historical(stock[0]))))

            if range == 'latest':
                for i, stock in enumerate(self.list_of_stocks):
                    processList.append(
                        mp.Process(target=self.parse_stock_obj, args=(stock[0], self.retrieve_latest(stock[0]))))

            printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
            for i, x in enumerate(processList):
                x.start()
                list_of_processes.append(x)
                if len(list_of_processes) > self.np:
                    for y in list_of_processes:
                        y.join()
                printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            # Cleanup
            for x in processList:
                x.join()
        except:
            insert_error_log("ERROR: Multiprocessor threw an error during data load")

    def retrieve_historical(self, ticker, years_back=2):
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        if start_date is not None:
            # https: // api.tiingo.com / tiingo / news?tickers = aapl, googl
            response = requests.get(f"https://api.tiingo.com/tiingo/news?tickers={ticker}&"
                                    f"startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
            if "Error:" in response.text:
                insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
                return "Error"
            else:
                return json.loads(response.text)
        else:
            return "Error"

    def retrieve_latest(self, ticker, years_back=2):
        pass

    def parse_stock_obj(self, ticker, data):
        pass


    def sentiment_analysis(self, parsed_news):
        # Instantiate the sentiment intensity analyzer
        # pip install vaderSentiment
        vader = SentimentIntensityAnalyzer()
        # Set column names
        columns = ['ticker', 'date', 'time', 'headline', 'link']
        # Convert the parsed_news list into a DataFrame called 'parsed_and_scored_news'

        parsed_and_scored_news = pd.DataFrame(parsed_news, columns=columns)

        # Iterate through the headlines and get the polarity scores using vader
        scores = parsed_and_scored_news['headline'].apply(vader.polarity_scores).tolist()

        # Convert the 'scores' list of dicts into a DataFrame
        scores_df = pd.DataFrame(scores)

        # Join the DataFrames of the news and the list of dicts
        parsed_and_scored_news = parsed_and_scored_news.join(scores_df, rsuffix='_right')

        # Convert the date column from string to datetime
        parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date
        # parsed_and_scored_news.head()
        return parsed_and_scored_news