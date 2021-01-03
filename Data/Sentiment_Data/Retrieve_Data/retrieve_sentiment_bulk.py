import csv
import math
import multiprocessing
import time
from json import JSONDecodeError
from multiprocessing.pool import ThreadPool

import Utility.multithreading as multi_threading
from dateutil import parser
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests.exceptions as exc
from Data.Technical_Data.Model.sentiment_model import sentiment_model
from Data.stock_list import stock_list
from config_read import config as con
from Database.Service.database import database
from Utility.string_manipulation import clean, list_to_database
from Interface.utility import printProgressBar
from Data.Technical_Data.Stock_Utility.date_helper import find_most_recent_date
from Database.Service.database import insert_error_log
import datetime
import requests
import json

# look into downloading data in csv
# running sentiment analysis
# preparing new csv for loading using other format
# loading data into table

# TODO finish creating a sentiment bulk loader that is faster than the other current loader
class retrieve_sentiment:
    def __init__(self):
        config = con()
        self.conn_sentiment = database().conn_sentiment
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()
        # self.list_of_stocks = [('AAPL',), ('IBM',), ('AAA',), ('GOOG',)]
        self.temp_csv_file_name = 'temp_sentiment.csv'
        self.size = 0
        self.cores = multiprocessing.cpu_count()

    def run_data_load(self):
        range = 'historical'
        url_list = []

        with open(f"./Data/Sentiment_Data/temp/{self.temp_csv_file_name}", mode='w', newline='') as data_download:
            data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # load data into csv
        for stock in self.list_of_stocks:
            url_list.append(self.build_url(stock[0], range))
        try:
            results = ThreadPool(self.cores - 1).imap_unordered(self.retrieve_data, url_list)
            l = len(self.list_of_stocks)
            printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
            for i, path in enumerate(results):
                printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
        except exc:
            print(exc)

    def build_url(self, ticker, range, years_back = 2):
        if range == 'latest':
            start_date = find_most_recent_date(ticker)
            if start_date is None:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        if range == 'historical':
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        url = f"https://api.tiingo.com/tiingo/news?tickers={ticker}&startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}"
        return url

    def retrieve_data(self, url):
        try:
            response = requests.get(url)
            self.size += float(response.headers['content-length']) / 1000000
            if "Error" not in response.text:
                try:
                    # TODO wait to parse obj and run through sentiment analysis until later, just save to csv
                    self.to_csv(json.loads(response.text))
                except JSONDecodeError:
                    print(JSONDecodeError)
        except requests.exceptions.RequestException as e:
            print(e)

    def to_csv(self, data):
        tickers, tags = "", ""
        try:
            with open(f"./Data/Sentiment_Data/temp/{self.temp_csv_file_name}", mode='a', newline='', encoding='utf-8') as data_download:
                data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for x in data:
                    for t in x['tickers']:
                        tickers += ":" + t
                    for t in x['tags']:
                        tags += ":" + t
                    data_writer.writerow([x['crawlDate'], x['publishedDate'], x['source'], x['url'],
                                          x['title'], tickers[1:], tags[1:], x['title'], x['description']])
        except exc:
            print(exc)

    def parse_sentiment_obj(self, data):
        sentiment_obj_list = []
        if data != "Error":
            try:
                for sent_json in data:
                    sentiment_obj = sentiment_model()
                    sentiment_obj.crawl_date = parser.parse(sent_json['crawlDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.published_date = parser.parse(sent_json['publishedDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.source = sent_json['source']
                    sentiment_obj.url = sent_json['url']
                    sentiment_obj.title = sent_json['title']
                    sentiment_obj.symbols = sent_json['tickers']
                    sentiment_obj.tags = sent_json['tags']
                    sentiment_obj.sentiment_data = self.sentiment_analysis([sent_json['description'], sent_json['title']])
                    sentiment_obj_list.append(sentiment_obj)
                return sentiment_obj_list
            except exc:
                insert_error_log("ERROR: Could not parse sentiment object during data load")
                # print(exc)
        else:
            return