
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
        # check data
        result = True
        try:
            cursor = self.conn_sentiment.cursor()
            sql_statement = f"UPDATE {stock_obj.symbol}_STK " \
                            f"SET open='{stock_obj.open}', close='{stock_obj.close}', " \
                            f"high='{stock_obj.high}', low='{stock_obj.low}', " \
                            f"adj_close='{stock_obj.adj_close}', volume='{stock_obj.volume}', dividend='{stock_obj.dividend}'," \
                            f"split='{stock_obj.split}', sma_ind='{stock_obj.sma_ind}' " \
                            f"WHERE dt='{stock_obj.date}' ;"
            cursor.execute(sql_statement)
            self.conn_stock.commit()
        except errors:
            print(errors)
            insert_error_log(f"ERROR UPDATING TECHNICAL DATA INTO DATABASE FOR {stock_obj.symbol} AT {stock_obj.date}")


    def insert_sentiment_obj(self, sentiment_obj: sentiment_model):
        # check data
        result = True
        try:
            tickers = list_to_database(sentiment_obj.symbols)
            tags = list_to_database(sentiment_obj.tags)
            cursor = self.conn_sentiment.cursor()
            result = self.check_if_exists(sentiment_obj.source, sentiment_obj.crawl_date, sentiment_obj.published_date, sentiment_obj.url, sentiment_obj.title)
            if result != False:
                # Compare list of tickers as well as tags to make sure they are the same
                sql_statement = f"UPDATE SENTIMENT_DATA " \
                                f"SET crawlDate='{sentiment_obj.crawl_date}', publishedDate='{sentiment_obj.published_date}', " \
                                f"tickers='{tickers}', tags='{tags}', source='{sentiment_obj.source}', url='{sentiment_obj.url}, " \
                                f"title='{sentiment_obj.title}', description='{sentiment_obj.description}', " \
                                f"sent_neg='{sentiment_obj.sentiment_data['negative']}', sent_pos='{sentiment_obj.sentiment_data['positive']}', " \
                                f"sent_neutral='{sentiment_obj.sentiment_data['neutral']}', sent_compound='{sentiment_obj.sentiment_data['compound']}'" \
                                f"WHERE publishedDate='{sentiment_obj.published_date}' AND url='{sentiment_obj.url}';"
                cursor.execute(sql_statement)
                self.conn_stock.commit()
            elif result == False:
                values = (sentiment_obj.crawl_date, sentiment_obj.published_date, tickers, tags, sentiment_obj.source, sentiment_obj.url,
                          sentiment_obj.title, sentiment_obj.description, sentiment_obj.sentiment_data['negative'], sentiment_obj.sentiment_data['positive'],
                          sentiment_obj.sentiment_data['neutral'], sentiment_obj.sentiment_data['compound'])
                sql_statement = f"INSERT INTO SENTIMENT_DATA (crawlDate, publishedDate, tickers, tags, course, url, title, description, " \
                                f"sent_neg, sent_pos, sent_neutral, sent_compound) " \
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_statement, values)
                self.conn_stock.commit()
        except errors as err:
            insert_error_log(f"ERROR INSERTING/UPDATING SENTIMENT DATA INTO DATABASE FOR {sentiment_obj.title} AT {sentiment_obj.source}")


    def check_if_exists(self, source, crawl_date, published_date, url, title):
        sql_statement = f"SELECT id, tickers, tags FROM SENTIMENT_DATA WHERE source='{source}' AND crawlDate='{crawl_date}' AND " \
                        f"publishedDate='{published_date}' AND url='{url}' AND title='{title}';"
        # sql_statement = f"SELECT IF( EXISTS( SELECT * FROM SENTIMENT_DATA WHERE source = '{source}' AND crawlDate = '{crawl_date}' AND " \
        #                 f"publishedDate = '{published_date}' AND url = '{url}' AND title = '{title}'), 1, 0);";
        try:
            cursor = self.conn_sentiment.cursor(buffered=True)
            cursor.execute(sql_statement)
            exists = cursor.fetchone()
        except errors as error:
            insert_error_log(f"ERROR CHECKING SENTIMENT DATA FOR DATABASE FOR {title} AT {source}")
        if exists is None:
            return False
        else:
            return exists