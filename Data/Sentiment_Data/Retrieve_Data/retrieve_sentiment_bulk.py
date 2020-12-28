import csv
import math
from langdetect import detect
from dateutil import parser
import mysql.connector as connect
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from Data.Technical_Data.Model.sentiment_model import sentiment_model
from Interface.utility import printProgressBar
from config_read import config as con
import requests
from Data.stock_list import stock_list
from Database.Service.database import insert_error_log
import datetime
import json
from multiprocessing.pool import ThreadPool
import multiprocessing
from time import time as timer
from Logger.logger import log_status
from Logger.logger import log_error
import time

# TODO clean up code/simplify and add proper logging and error handling
class retrieve_technical_bulk:
    def __init__(self):
        config = con()
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()
        self.errors = 0
        self.temp_csv_file_name = "temp_data_load_sentiment.csv"
        self.size = 0
        self.cores = multiprocessing.cpu_count()

    def run_data_load(self, range='latest'):
        url_list = []
        start = timer()
        try:
            with open(self.temp_csv_file_name, mode='w', newline='') as data_download:
                data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                data_writer.writerow(['crawlDate', 'publishedDate', 'tickers', 'tags', 'source', 'url', 'title', 'sent_neg', 'sent_pos', 'sent_neutral', 'sent_compound'])
        except:
            log_error("Error creating csv table for sentiment data", "retrieve_sentiment.csv_writer", print=False, email=True)

        try:
            for i, entry in enumerate(self.list_of_stocks):
                url_list.append((entry[0], self.build_url(entry[0], range)))
        except:
            log_error("Error creating sentiment data urls", "retrieve_sentiment.url_list", print=False)

        try:
            results = ThreadPool(self.cores - 1).imap_unordered(self.retrieve_data, url_list)
            l = len(self.list_of_stocks)
            printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
            for i, path in enumerate(results):
                printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
        except:
            log_error("Error in ThreadPool", "retrieve_sentiment.run_data_load.ThreadPool", print=False, email=True)

        try:
            pass
        # self.bulk_load_data()
        except:
            print("ERROR")
        log_status(f"Completed Sentiment Data Load in {timer() - start} seconds", "retrieve_sentiment.run_data_load.py", print=True)

    def build_url(self, ticker, range, years_back = 2):
        if range == "historical":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        elif range == "latest":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        elif range == "month":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        url = f"https://api.tiingo.com/tiingo/news?tickers={ticker.replace('_','-')}&startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}"
        return url

    # TODO may need to convert tags and symbols to a different format
    def to_csv(self, obj_list):
        try:
            with open(self.temp_csv_file_name, mode='a', newline='', encoding='utf-8', errors='ignore') as data_download:
                data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for x in obj_list:
                    data_writer.writerow([f'{x.crawl_date}', f'{x.published_date}', f'{x.symbols}', f'{x.tags}', f'{x.source}',
                                          f'{x.title}', f'{x.url}', x.sentiment_data['negative'], x.sentiment_data['neutral'],
                                          x.sentiment_data['positive'], x.sentiment_data['compound']])
        except:
            log_error("Error writing sentiment data to csv", "retrieve_sentiment.to_csv", print=False, email=True)

    def parse_sentiment_obj(self, data):
        sentiment_obj_list = []
        if "Error" not in data:
            try:
                for sent_json in data:
                    sentiment_obj = sentiment_model()
                    sentiment_obj.crawl_date = parser.parse(sent_json['crawlDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.published_date = parser.parse(sent_json['publishedDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.source = sent_json['source']
                    sentiment_obj.url = sent_json['url']
                    sentiment_obj.title = f"{sent_json['title']}"
                    sentiment_obj.symbols = sent_json['tickers']
                    sentiment_obj.tags = sent_json['tags']
                    if sent_json['description'] != "":
                        lang_description = detect(sent_json['description'])
                        lang_title = detect(sentiment_obj.title)
                        if lang_description == 'en' and lang_title == 'en':
                            sentiment_obj.sentiment_data = self.sentiment_analysis([sent_json['description'], sent_json['title']])
                            sentiment_obj_list.append(sentiment_obj)
                return sentiment_obj_list
            except:
                log_error("Could not parse sentiment object during data load", "retrieve_sentiment.parse_sentiment_obj")
        else:
            return

    def retrieve_data(self, url_data):
        ticker = url_data[0]
        url = url_data[1]
        try:
            response = requests.get(url)
            self.size += float(response.headers['content-length'])/1000000
            if "Error" not in response.text:
                self.to_csv(self.parse_sentiment_obj(json.loads(response.text)))
        except:
            self.errors += 1
            print(f"ERROR {ticker}")

    def sentiment_analysis(self, data):
        neg, neu, pos, compound = 0, 0, 0, 0
        for words in data:
            vader = SentimentIntensityAnalyzer()
            columns = ['entry']
            scored_data = pd.DataFrame(data, columns=columns)
            scores = scored_data['entry'].apply(vader.polarity_scores).tolist()
            scores_df = pd.DataFrame(scores)
            scored_data = scored_data.join(scores_df, rsuffix='_right')
            neg += flt_num(scored_data.iloc[0].get('neg'))
            neu += flt_num(scored_data.iloc[0].get('neu'))
            pos += flt_num(scored_data.iloc[0].get('pos'))
            compound += flt_num(scored_data.iloc[0].get('compound'))
        sent_dict = {
            'negative' : neg/len(data),
            'neutral' : neu/len(data),
            'positive' : pos/len(data),
            'compound' : compound/len(data)
        }
        return sent_dict

    def bulk_load_data(self):
        config = con()
        try:
            conn_admin = connect.connect(
                host=f"{config.db_host}",
                user=f"{config.db_user_root}",
                password=f"{config.db_pass_root}"
            )
            conn = connect.connect(
                host=f"{config.db_host}",
                user=f"{config.db_user}",
                password=f"{config.db_pass}",
                database=f"{config.stock_db_name}",
                allow_local_infile=True
            )

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = ON;")
            cursor.close()

            # Create temporary table like the table being inserted into
            cursor = conn.cursor()
            cursor.execute(f"CREATE TEMPORARY TABLE temporary_technical_data_table LIKE stock_technical_data.stock_data;")
            cursor.close()

            # Load data into temporary table
            cursor = conn.cursor()
            cursor.execute(f"""LOAD DATA LOCAL INFILE '{self.temp_csv_file_name}' 
                INTO TABLE temporary_technical_data_table 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS;""")
            cursor.close()

            # Load data into proper table
            cursor = conn.cursor()
            cursor.execute(f"""INSERT INTO stock_technical_data.stock_data
                SELECT * FROM temporary_technical_data_table
                ON DUPLICATE KEY UPDATE ticker = VALUES(ticker), dt = VALUES(dt), open = VALUES(open), close = VALUES(close), high = VALUES(high), 
                low = VALUES(low), adj_close = VALUES(adj_close), volume = VALUES(volume), split = VALUES(split), dividend = VALUES(dividend);""")
            cursor.close()

            # Drop temporary table
            cursor = conn.cursor()
            cursor.execute("DROP TEMPORARY TABLE temporary_technical_data_table;")
            cursor.close()

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = OFF;")
            cursor.close()
            conn.close()
            conn_admin.close()
        except:
            print("ERROR")

def clean_sql(str):
    clean_str = str.replace("'", "''")
    clean_str = clean_str.replace('"', '""')
    return clean_str

def flt_num(number):
    if math.isnan(number):
        return float('NaN')
    else:
        return round(float(number),3)