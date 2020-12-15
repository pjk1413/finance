import math
from dateutil import parser
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests.exceptions as exc
from Data.Technical_Data.Model.sentiment_model import sentiment_model
from Data.stock_list import stock_list
from Database.Service.sentiment_service import sentiment_service as service
from Data.config_read import config as con
from Database.database import database
from Utility.string_manipulation import clean
from Interface.utility import printProgressBar
from Data.Technical_Data.Stock_Utility.date_helper import find_most_recent_date
import multiprocessing as mp
from Database.database import insert_error_log
import datetime
import requests
import json

# TODO allow data retrieval for multiple tickers at once
class retrieve_sentiment_data:
    def __init__(self):
        config = con()
        self.conn_stock = database().conn_stock
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()
        # self.list_of_stocks = [('GM',)]

    def run_data_load(self, range='latest'):
        processList = []
        list_of_processes = []
        l = len(self.list_of_stocks)
        try:
            if range == 'historical':
                for stock in self.list_of_stocks:
                    processList.append(
                        mp.Process(target=self.parse_sentiment_obj, args=(self.retrieve_historical(stock[0]))))

            if range == 'latest':
                for i, stock in enumerate(self.list_of_stocks):
                    processList.append(
                        mp.Process(target=self.parse_sentiment_obj, args=(self.retrieve_latest(stock[0]))))

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
            response = requests.get(f"https://api.tiingo.com/tiingo/news?tickers={ticker}&"
                                    f"startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
            if "Error:" in response.text:
                print("Error")
                insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
                return "Error"
            else:
                return [json.loads(response.text)]
        else:
            return "Error"

    def retrieve_latest(self, ticker, years_back=2):
        start_date = find_most_recent_date(ticker)
        if start_date is None:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        else:
            start_date = start_date[0].date()
        response = requests.get(f"https://api.tiingo.com/tiingo/news?tickers={ticker}&"
                                f"startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
        if "Error:" in response.text:
            insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
            return "Error"
        else:
            return json.loads(response.text)

    def parse_sentiment_obj(self, data):
        if data != "Error":
            try:
                for sent_json in data:
                    sentiment_obj = sentiment_model()
                    sentiment_obj.crawl_date = parser.parse(sent_json['crawlDate'])
                    sentiment_obj.published_date = parser.parse(sent_json['publishedDate'])
                    sentiment_obj.source = sent_json['source']
                    sentiment_obj.url = sent_json['url']
                    sentiment_obj.title = sent_json['title']
                    sentiment_obj.description = sent_json['description']
                    sentiment_obj.symbols = sent_json['tickers']
                    sentiment_obj.tags = sent_json['tags']
                    sentiment_obj.sentiment_data = self.sentiment_analysis([sent_json['description'], sent_json['title']])
                    service().insert_sentiment_obj(sentiment_obj)
            except exc:
                insert_error_log("ERROR: Could not parse sentiment object during data load")
                # print(exc)
        else:
            return


    def sentiment_analysis(self, data):
        neg, neu, pos, compound = 0, 0, 0, 0
        for words in data:
            prep_words = [[words]]

            # Instantiate the sentiment intensity analyzer
            # pip install vaderSentiment
            vader = SentimentIntensityAnalyzer()
            # Set column names
            columns = ['entry']
            # Convert the parsed_news list into a DataFrame called 'parsed_and_scored_news'
            scored_data = pd.DataFrame(data, columns=columns)
            # Iterate through the headlines and get the polarity scores using vader
            scores = scored_data['entry'].apply(vader.polarity_scores).tolist()
            # Convert the 'scores' list of dicts into a DataFrame
            scores_df = pd.DataFrame(scores)
            # Join the DataFrames of the news and the list of dicts
            scored_data = scored_data.join(scores_df, rsuffix='_right')
            # Convert the date column from string to datetime
            # parsed_and_scored_news['date'] = pd.to_datetime(parsed_and_scored_news.date).dt.date
            # parsed_and_scored_news.head()
            neg += self.flt_num(scored_data.iloc[0].get('neg'))
            neu += self.flt_num(scored_data.iloc[0].get('neu'))
            pos += self.flt_num(scored_data.iloc[0].get('pos'))
            compound += self.flt_num(scored_data.iloc[0].get('compound'))
        sent_dict = {
            'negative' : neg/len(data),
            'neutral' : neu/len(data),
            'positive' : pos/len(data),
            'compound' : compound/len(data)
        }
        return sent_dict

    def flt_num(self, number):
        if math.isnan(number):
            return float('NaN')
        else:
            return round(float(number),3)