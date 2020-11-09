import mysql.connector as connect
from Data.config_read import config as get_values
import datetime
import mysql


class database:
    def __init__(self):
        config = get_values()
        # self.data_check = self.validate_data()
        self.root_user = config.db_user_root
        self.root_pass = config.db_pass_root
        self.user = config.db_user
        self.password = config.db_pass
        self.host = config.db_host
        self.stock_db = config.stock_db_name
        self.sentiment_db = config.sentiment_db_name
        self.conn_finance = connect.connect(
            host=f"{self.host}",
            user=f"{self.user}",
            password=f"{self.password}",
            database=f"{self.stock_db}"
        )
        self.conn_sentiment = connect.connect(
            host=f"{self.host}",
            user=f"{self.user}",
            password=f"{self.password}",
            database=f"{self.sentiment_db}"
        )


    def insert_error_log(self, statement):
        sql_statement = "INSERT INTO ERROR_LOG (dt, description) " \
                        "VALUES (%s, %s)"
        values = (datetime.datetime.now(), statement)

        cursor = self.conn_finance.cursor()
        try:
            cursor.execute(sql_statement, values)
            return True
        except:
            return False









