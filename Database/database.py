import mysql.connector as connect
from Data.config_read import config as get_values
import datetime
import mysql.connector.errors as error


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
        self.utility_db = config.utility_db_name
        try:
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
            self.conn_utility = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.utility_db}"
            )
        except error:
            print("Error connecting to database")


    def insert_status_log(self, statement):
        sql_statement = "INSERT INTO STATUS_TBL (dt, statement) " \
                        "VALUES (%s, %s)"
        values = (datetime.datetime.now(), statement)
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement, values)
            self.conn_utility.commit()
            return True
        except error:
            print(error)
            return False

    def insert_error_log(self, statement):
        sql_statement = "INSERT INTO error_log (dt, description) " \
                        "VALUES (%s, %s)"
        values = (datetime.datetime.now(), statement)

        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement, values)
            self.conn_utility.commit()
            return True
        except error:
            print(error)
            return False









