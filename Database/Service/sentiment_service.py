
from Database.database import database
import mysql.connector.errors as errors
from Database.database import insert_error_log
from Data.Technical_Data.Model.sentiment_model import sentiment_model
from Utility.string_manipulation import list_to_database

class sentiment_service:
    def __init__(self):
        self.conn_sentiment = database().conn_sentiment
        pass


    def update_sentiment_object(self, sentiment_obj: sentiment_model):
        try:
            tickers = list_to_database(sentiment_obj.symbols)
            tags = list_to_database(sentiment_obj.tags)
            cursor = self.conn_sentiment.cursor()
            sql_statement = f"UPDATE SENTIMENT_DATA " \
                            f"SET crawlDate='{sentiment_obj.crawl_date}', publishedDate='{sentiment_obj.published_date}', " \
                            f"tickers='{tickers}', tags='{tags}', source='{sentiment_obj.source}', url='{sentiment_obj.url}, " \
                            f"title='{sentiment_obj.title}', description='{sentiment_obj.description}', " \
                            f"sent_neg='{sentiment_obj.sentiment_data['negative']}', sent_pos='{sentiment_obj.sentiment_data['positive']}', " \
                            f"sent_neutral='{sentiment_obj.sentiment_data['neutral']}', sent_compound='{sentiment_obj.sentiment_data['compound']}'" \
                            f"WHERE publishedDate='{sentiment_obj.published_date}' AND url='{sentiment_obj.url}';"
            cursor.execute(sql_statement)
            self.conn_stock.commit()
        except errors:
            print(errors)
            insert_error_log(f"ERROR UPDATING SENTIMENT DATA INTO DATABASE FOR {sentiment_obj.source} ON {sentiment_obj.published_date}")


    def insert_sentiment_obj(self, sentiment_obj: sentiment_model):
        result = True
        try:
            # TODO check length of tickers and tags and limit to under 1000 characters each
            tickers = list_to_database(sentiment_obj.symbols)
            tags = list_to_database(sentiment_obj.tags)
            cursor = self.conn_sentiment.cursor()
            result = self.check_if_exists(sentiment_obj.source, sentiment_obj.crawl_date, sentiment_obj.published_date, sentiment_obj.url)
            if result != False:
                sql_statement = f"UPDATE SENTIMENT_DATA " \
                                f"SET crawlDate='{sentiment_obj.crawl_date}', publishedDate='{sentiment_obj.published_date}', " \
                                f"tickers='{tickers}', tags='{tags}', source='{sentiment_obj.source}', url='{sentiment_obj.url}, " \
                                f"title='{sentiment_obj.title}', description='{sentiment_obj.description}', " \
                                f"sent_neg='{sentiment_obj.sentiment_data['negative']}', sent_pos='{sentiment_obj.sentiment_data['positive']}', " \
                                f"sent_neutral='{sentiment_obj.sentiment_data['neutral']}', sent_compound='{sentiment_obj.sentiment_data['compound']}'" \
                                f"WHERE publishedDate='{sentiment_obj.published_date}' AND url='{sentiment_obj.url}';"
                cursor.execute(sql_statement)
                self.conn_sentiment.commit()
            elif result == False:

                values = (sentiment_obj.crawl_date, sentiment_obj.published_date, tickers, tags, sentiment_obj.source, sentiment_obj.url,
                          sentiment_obj.title, sentiment_obj.description, sentiment_obj.sentiment_data['negative'], sentiment_obj.sentiment_data['positive'],
                          sentiment_obj.sentiment_data['neutral'], sentiment_obj.sentiment_data['compound'])
                sql_statement = f"INSERT INTO SENTIMENT_DATA (crawlDate, publishedDate, tickers, tags, source, url, title, description, " \
                                f"sent_neg, sent_pos, sent_neutral, sent_compound) " \
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_statement, values)
                self.conn_sentiment.commit()
                self.insert_sentiment_reference(sentiment_obj)
        except errors as err:
            insert_error_log(f"ERROR INSERTING/UPDATING SENTIMENT DATA INTO DATABASE FOR {sentiment_obj.title} AT {sentiment_obj.source}")


    def insert_sentiment_reference(self, sentiment_obj: sentiment_model):
        sql_select_statement = f"SELECT id FROM SENTIMENT_DATA WHERE url='{sentiment_obj.url}';"
        try:
            cursor = self.conn_sentiment.cursor(buffered=True)
            cursor.execute(sql_select_statement)
            id = cursor.fetchone()
        except:
            insert_error_log(f"ERROR FINDING NEW SENTIMENT ROW ON {sentiment_obj.source} AT {sentiment_obj.published_date}")

        if id is None:
            id = 1
        entries = len(sentiment_obj.symbols)
        sql_insert_statement = "INSERT INTO SENTIMENT_DATA_REFERENCE (ticker, SENTIMENT_ID) " \
                        "VALUES "
        for ticker in sentiment_obj.symbols:
            if not self.check_if_relationship_exists(id[0], ticker):
                entries -= 1
                sql_insert_statement += f"('{ticker}', {id[0]}), "
        sql_insert_statement = sql_insert_statement[0:len(sql_insert_statement)-2] + ";"
        if entries > 0:
            try:
                cursor = self.conn_sentiment.cursor()
                cursor.execute(sql_insert_statement)
                self.conn_sentiment.commit()
            except:
                insert_error_log(f"ERROR INSERTING NEW SENTIMENT ROW REFERENCE ON {sentiment_obj.source} AT {sentiment_obj.published_date}")


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
        if exists[0] is 0:
            return False
        else:
            return True