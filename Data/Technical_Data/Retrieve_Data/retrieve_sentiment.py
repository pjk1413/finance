import math
import time
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

# TODO allow data retrieval for multiple tickers at once
# TODO clean up class to make it more streamlined, less repetition
class retrieve_sentiment_data:
    def __init__(self):
        config = con()
        self.conn_stock = database().conn_stock
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()

    # TODO look into pooling downloads or running the entire function through a multiprocessor
    def run_data_load(self, range='latest'):
        start = time.perf_counter()
        if range == 'latest':
            l = len(self.list_of_stocks)
            printProgressBar(0, l, prefix=f'Sentiment Progress:', suffix='Complete', length=50)
            for i, entry in enumerate(self.list_of_stocks):
                sentiment_obj_list = self.parse_sentiment_obj(self.retrieve_latest(entry[0]))
                sql_statement_list = []
                for obj in sentiment_obj_list:
                    sql_statement_list.append(self.insert_sentiment_obj(obj))
                    sql_statement_list.append(self.insert_sentiment_reference(obj))
                threader = multi_threading.Multi_Threading(sql_statement_list, "sentiment")
                threader.run_insert()
                printProgressBar(i + 1, l, prefix=f'Current: {entry[0]} - Progress:', suffix='Complete', length=50)

        if range == 'historical':
            l = len(self.list_of_stocks)
            printProgressBar(0, l, prefix='Sentiment Progress:', suffix='Complete', length=50)
            for i, entry in enumerate(self.list_of_stocks):
                sentiment_obj_list = self.parse_sentiment_obj(self.retrieve_historical(entry[0]))
                sql_statement_list = []
                # TODO use multiprocessor to speed up building lists here
                for obj in sentiment_obj_list:
                    sql_statement_list.append(self.insert_sentiment_obj(obj))
                    sql_statement_list.append(self.insert_sentiment_reference(obj))
                threader = multi_threading.Multi_Threading(sql_statement_list, "sentiment")
                threader.run_insert()
                printProgressBar(i + 1, l, prefix=f'Current: {entry[0]} - Progress:', suffix='Complete', length=50)
        finish = time.perf_counter()
        print(f"Downloaded stock sentiment data in {finish - start} seconds \n")

    def retrieve_historical(self, ticker, years_back=2):
        start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        if start_date is not None:
            # TODO combine tickers into groups of 5 to grab more data at once
            # TODO look into storing data as a text file first before inserting into database
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
        start_date = find_most_recent_date(ticker)
        if start_date is None:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        else:
            start_date = start_date[0].date()
        # TODO combine tickers into groups of 5 to grab more data at once
        # TODO look into storing data as a text file first before inserting into database
        response = requests.get(f"https://api.tiingo.com/tiingo/news?tickers={ticker}&"
                                f"startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}")
        if "Error:" in response.text:
            insert_error_log(f"ERROR: {clean(response.text).replace('Error: ', '')}")
            return "Error"
        else:
            return json.loads(response.text)

    def parse_sentiment_obj(self, data):
        sentiment_obj_list = []
        if data != "Error":
            try:
                for sent_json in data:
                    # print(sent_json['description'])
                    sentiment_obj = sentiment_model()
                    sentiment_obj.crawl_date = parser.parse(sent_json['crawlDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.published_date = parser.parse(sent_json['publishedDate']).strftime('%Y-%m-%d %H:%M:%S')
                    sentiment_obj.source = sent_json['source']
                    sentiment_obj.url = sent_json['url']
                    sentiment_obj.title = sent_json['title']
                    sentiment_obj.description = sent_json['description']
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

    # TODO may need to update the update statement to include more information
    # TODO may need to update how we limit data entering columns
    def insert_sentiment_obj(self, sentiment_obj: sentiment_model):
        tickers = clean_sql(list_to_database(sentiment_obj.symbols))[:1999]
        tags = clean_sql(list_to_database(sentiment_obj.tags))[:1999]
        description = clean_sql(sentiment_obj.description)
        title = clean_sql(sentiment_obj.title)
        sql_statement = "INSERT INTO SENTIMENT_DATA (crawlDate, publishedDate, tickers, tags, source, url, title, description, " \
            "sent_neg, sent_pos, sent_neutral, sent_compound) " \
            f"VALUES ('{sentiment_obj.crawl_date}', '{sentiment_obj.published_date}', '{tickers}', '{tags}', " \
            f"'{sentiment_obj.source}', '{sentiment_obj.url}', '{title}', '{description}', " \
            f"{sentiment_obj.sentiment_data['negative']}, {sentiment_obj.sentiment_data['positive']}, " \
            f"{sentiment_obj.sentiment_data['neutral']}, {sentiment_obj.sentiment_data['compound']}) " \
            "ON DUPLICATE KEY UPDATE " \
            "crawlDate=VALUES(crawlDate), publishedDate=VALUES(publishedDate), " \
            "tickers=VALUES(tickers), tags=VALUES(tags), source=VALUES(source), " \
            "url=VALUES(url), title=VALUES(title);"

        return sql_statement

    def insert_sentiment_reference(self, sentiment_obj: sentiment_model):
        sql_statement = "INSERT INTO stock_sentiment_data.sentiment_data_reference (ticker, SENTIMENT_ID) VALUES "
        for ticker in sentiment_obj.symbols:
            sql_statement += f"('{ticker}', (SELECT id FROM stock_sentiment_data.sentiment_data WHERE url='{sentiment_obj.url}' " \
                 f"AND publishedDate='{sentiment_obj.published_date}' LIMIT 1)),"
        sql_statement = sql_statement[:-1]
        sql_statement += ";"
        return sql_statement


def clean_sql(str):
    clean_str = str.replace("'", "''")
    clean_str = clean_str.replace('"', '""')
    return clean_str

def flt_num(self, number):
    if math.isnan(number):
        return float('NaN')
    else:
        return round(float(number),3)