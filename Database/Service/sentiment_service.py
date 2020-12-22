
from Database.database import database
import mysql.connector.errors as errors
from Database.database import insert_error_log
from Data.Technical_Data.Model.sentiment_model import sentiment_model
from Utility.string_manipulation import list_to_database
import pymysql.converters as con
from pymysql.converters import escape_string

class sentiment_service:
    def __init__(self):
        self.conn_sentiment = database().conn_sentiment
        pass

    def insert_sentiment_obj(self, sentiment_obj: sentiment_model):
        result = True
        tickers = list_to_database(sentiment_obj.symbols)
        tags = list_to_database(sentiment_obj.tags)

        sql_statement = f"INSERT INTO SENTIMENT_DATA (crawlDate, publishedDate, tickers, tags, source, url, title, description, " \
                        f"sent_neg, sent_pos, sent_neutral, sent_compound) " \
                        f"VALUES ('{sentiment_obj.crawl_date}', '{sentiment_obj.published_date}', '{tickers}', '{tags}', " \
                        f"'{sentiment_obj.source}', '{sentiment_obj.url}', '{cl(sentiment_obj.title)}', '{cl(sentiment_obj.description)}', " \
                        f"{sentiment_obj.sentiment_data['negative']}, {sentiment_obj.sentiment_data['positive']}, " \
                        f"{sentiment_obj.sentiment_data['neutral']}, {sentiment_obj.sentiment_data['compound']}) " \
                        f"ON DUPLICATE KEY UPDATE " \
                        f"crawlDate='{sentiment_obj.crawl_date}', publishedDate='{sentiment_obj.published_date}', tickers='{tickers}', " \
                        f"tags='{tags}', source='{sentiment_obj.source}', url='{sentiment_obj.url}', title='{cl(sentiment_obj.title)}', " \
                        f"description='{cl(sentiment_obj.description)}';"
        return sql_statement


    def insert_sentiment_reference(self, sentiment_obj: sentiment_model):
        sql_statement = "INSERT INTO stock_sentiment_data.sentiment_data_reference (ticker, SENTIMENT_ID) VALUES "
        for ticker in sentiment_obj.symbols:
            sql_statement += f"('{ticker}', (SELECT id FROM stock_sentiment_data.sentiment_data WHERE url='{sentiment_obj.url}' " \
                        f"AND publishedDate='{sentiment_obj.published_date}' LIMIT 1)), "
        sql_statement = sql_statement[:-2]
        sql_statement += ";"
        return sql_statement


    def check_if_exists(self, source, crawl_date, published_date, url):
        sql_statement = f"SELECT id, tickers, tags FROM SENTIMENT_DATA WHERE source='{source}' AND crawlDate='{crawl_date}' AND " \
                        f"publishedDate='{published_date}' AND url='{url}';"
        try:
            cursor = self.conn_sentiment.cursor(buffered=True)
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except errors as error:
            insert_error_log(f"ERROR CHECKING SENTIMENT DATA FOR DATABASE FOR {source} AT {published_date}")
        if exists is None:
            return False
        else:
            return exists


    def check_if_relationship_exists(self, id, ticker):
        # returns 1 if exists, 0 if it doesn't
        sql_statement = f"SELECT EXISTS(SELECT * FROM SENTIMENT_DATA_REFERENCE WHERE SENTIMENT_ID='{id}' AND ticker='{ticker}');"
        try:
            cursor = self.conn_sentiment.cursor(buffered=True)
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except errors as error:
            insert_error_log(f"ERROR CHECKING SENTIMENT DATA FOR {id} AND {ticker} IN SENTIMENT DATA REFERENCE")
        if exists[0] == 0:
            return False
        else:
            return True

def cl(str):
    return escape_string(str)
    # list_to_remove = [
    #     '"', "'", '`', '@', '#', '%', '^', '&', '*', 'α'
    # ]
    # for x in list_to_remove:
    #     str = str.replace(x, '')
    # # print("XX : " + str)
    # return str
