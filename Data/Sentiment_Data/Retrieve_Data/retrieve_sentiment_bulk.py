import csv
import math
import mysql.connector as connect
import multiprocessing
import time
from json import JSONDecodeError
from multiprocessing.pool import ThreadPool
import mysql.connector.errors as err
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
        self.timer = 0

    def run_data_load(self, range='latest'):
        url_list = []
        with open(self.temp_csv_file_name, mode='w', newline='', encoding='utf-8') as data_download:
            data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            data_writer.writerow(['crawlDate', 'publishedDate', 'tickers', 'tags', 'source', 'title', 'url',
                                    'sent_neg', 'sent_neutral', 'sent_pos', 'sent_compound'])

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
        self.bulk_load_data()
        print(self.timer)

    def build_url(self, ticker, range):
        if range == "historical":
            start_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')
        elif range == "latest":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        elif range == "month":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        url = f"https://api.tiingo.com/tiingo/news?tickers={ticker}&startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}"
        return url

    def retrieve_data(self, url):
        try:
            response = requests.get(url)
            self.size += float(response.headers['content-length']) / 1000000
            if "Error" not in response.text:
                try:
                    # TODO wait to parse obj and run through sentiment analysis until later, just save to csv
                    self.to_csv(self.parse_sentiment_obj(json.loads(response.text)))
                except JSONDecodeError:
                    print(JSONDecodeError)
        except requests.exceptions.RequestException as e:
            print(e)

    def to_csv(self, data):
        try:
            with open(self.temp_csv_file_name, mode='a', newline='', encoding='utf-8') as data_download:
                data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for x in data:
                    data_writer.writerow([x.crawl_date, x.published_date, x.symbols, x.tags, x.source, x.title, x.url,
                        x.sent_data['negative'], x.sent_data['neutral'], x.sent_data['positive'], x.sent_data['compound']])
        except exc:
            print(exc)

    def parse_sentiment_obj(self, data):
        sentiment_obj_list = []
        if data != "Error":
            try:
                for sent_json in data:
                    tickers, tags = "", ""
                    for t in sent_json['tickers']:
                        tickers += ":" + t
                    for t in sent_json['tags']:
                        tags += ":" + t
                    sentiment_obj = sentiment_model()
                    sentiment_obj.crawl_date = parser.parse(sent_json['crawlDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.published_date = parser.parse(sent_json['publishedDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.source = sent_json['source']
                    sentiment_obj.url = sent_json['url']
                    sentiment_obj.title = sent_json['title']
                    sentiment_obj.symbols = tickers
                    sentiment_obj.tags = tags
                    sentiment_obj.sent_data = self.sentiment_analysis([sent_json['description']])
                    sentiment_obj_list.append(sentiment_obj)
                return sentiment_obj_list
            except exc:
                insert_error_log("ERROR: Could not parse sentiment object during data load")
        else:
            return

    def sentiment_analysis(self, data):
        start = time.perf_counter()
        neg, neu, pos, compound = 0, 0, 0, 0
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
        finish = time.perf_counter()
        self.timer += (finish - start)
        return sent_dict

    def bulk_load_data(self):
        TEMP_TABLE = 'TEMPORARY_SENTIMENT_DATA_TABLE'

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
                database=f"{config.sentiment_db_name}",
                allow_local_infile=True,
                charset='utf8'
            )

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = true;")
            cursor.close()

            # Create temporary table like the table being inserted into
            cursor = conn.cursor()
            cursor.execute(f"CREATE TEMPORARY TABLE {TEMP_TABLE} LIKE stock_sentiment_data.sentiment_data;")
            cursor.close()

            # Load data into temporary table
            cursor = conn.cursor()
            cursor.execute(f"""LOAD DATA LOCAL INFILE '{self.temp_csv_file_name}' 
                IGNORE  
                INTO TABLE {TEMP_TABLE} 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"' 
                LINES TERMINATED BY '\n' 
                IGNORE 1 ROWS;""")
            cursor.close()

            # Load data into proper table
            cursor = conn.cursor()
            cursor.execute(f"""INSERT INTO stock_sentiment_data.sentiment_data 
                SELECT * FROM {TEMP_TABLE} 
                ON DUPLICATE KEY UPDATE crawlDate=VALUES(crawlDate), publishedDate=VALUES(publishedDate), tickers=VALUES(tickers),
                tags=VALUES(tags), source=VALUES(source), url=VALUES(url), title=VALUES(title), sent_neg=VALUES(sent_neg), 
                sent_neutral=VALUES(sent_neutral), sent_pos=VALUES(sent_pos), sent_compound=VALUES(sent_compound);""")
            conn.commit()
            cursor.close()

            # Drop temporary table
            cursor = conn.cursor()
            cursor.execute(f"DROP TEMPORARY TABLE {TEMP_TABLE};")
            cursor.close()

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = false;")
            cursor.close()
            conn.close()
            conn_admin.close()
        except err:
            print(err)
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