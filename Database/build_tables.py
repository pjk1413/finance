from Database.database import database
from Database.crud_func import crud
from Database.build_database import build_database as build
import Data.build_stock_list as stk_list
import mysql.connector as connect
import Data.config_read as config
import Interface.utility as utility
import mysql.connector.errors as errors
import time


class build_tables:
    def __init__(self):
        con = config.config()
        self.conn_utility = database().conn_utility
        self.conn = database().conn_finance
        self.crud = crud()
        self.host = con.db_host
        self.root_user = con.db_user_root
        self.root_pass = con.db_pass_root
        self.sentiment_db_name = con.sentiment_db_name
        self.stock_db_name = con.stock_db_name

    def build_tables(self):
        print("Error Log table created successfully") if self.create_error_log_table() else print("FAILED CREATING ERROR LOG TABLE")
        print("Status table created successfully") if self.create_status_table() else print("FAILED CREATING STATUS TABLE")
        print("Stock List table created successfully") if self.create_stock_list_table() else print("FAILED CREATING STOCK LIST TABLE")
        print("Stock List loaded into the database") if stk_list.stock_list().list_to_db() else print("FAILED LOADING STOCK LIST INTO DATABASE")
        print("All Stock tables created successfully") if self.create_all_stock_tables() else print("FAILED CREATING ALL STOCK TABLES")
        print("All Sentiment tables created successfully") if self.create_all_sentiment_tables() else print("FAILED CREATING ALL SENTIMENT TABLES")
        print("""
        ------------------
        ALL TABLES CREATED
        ------------------
        """)


    def create_status_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS STATUS_TBL (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, statement VARCHAR(255);"
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            return True
        except:
            print("ERROR CREATING ERROR LOG TABLE - NO ERROR LOG TABLE EXISTS")
            return False


    def create_error_log_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS error_log (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, description VARCHAR(255));"

        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            return True
        except:
            print("ERROR CREATING ERROR LOG TABLE - NO ERROR LOG TABLE EXISTS")
            return False


    def create_stock_list_table(self):
        table_name = "STOCK_LIST_TBL"

        sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        f"ticker VARCHAR(8), description VARCHAR(255), sector VARCHAR(60), industry VARCHAR(60), market VARCHAR(6));"

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            return True
        except:
            db = database()
            db.insert_error_log(f"ERROR CREATING TABLE [{table_name}]: {self.conn.get_warnings}")
            return False


    def grant_access_to_stock_tables(self):
        build().grant_all_user()


    def create_all_sentiment_tables(self):
        result = True
        stock_list = self.crud.get_list_of_stocks()

        l = len(stock_list)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for i, stock in enumerate(stock_list):
            sql_statement_sentiment = f"CREATE TABLE IF NOT EXISTS {stock[0]}_SENT (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                      f"dt DATETIME, headline VARCHAR(500), sent_neg FLOAT(8,4), sent_neutral FLOAT(8,4), " \
                                      f"sent_pos FLOAT(8,4), sent_compound FLOAT(8,4), url VARCHAR(350));"


            try:
                conn = connect.connect(
                    host=self.host,
                    user=self.root_user,
                    password=self.root_pass,
                    database=self.sentiment_db_name
                )
                cursor = conn.cursor()
                cursor.execute(sql_statement_sentiment)
                conn.close()
                utility.printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            except errors:
                db = database()
                db.insert_error_log(f"ERROR CREATING TABLE PRICES {stock}: {self.conn.get_warnings}")
                print(f"ERROR CREATING TABLE SENTIMENT {stock[0]}: {conn.get_warnings}")
                result = False

        return result


    def create_predict_tables(self):
        # Create all tables with all variables needed to model data appropriatley
        result = True

        sql_statement_linear_regression = "CREATE TABLE IF NOT EXISTS linear_regression (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                          "dt DATETIME, stock VARCHAR(10), description VARCHAR(100), b FLOAT(8,4), x FLOAT(8,4)"


    def create_all_stock_tables(self):
        result = True
        stock_list = self.crud.get_list_of_stocks()

        l = len(stock_list)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for i, stock in enumerate(stock_list):

            # Look into data types for indicator values and certain metrics that could be added in
            sql_statement = f"CREATE TABLE IF NOT EXISTS {stock[0]}_STK (id INT AUTO_INCREMENT PRIMARY KEY, " \
                            f"dt DATETIME, open FLOAT(8,4), close FLOAT(8,4), high FLOAT(8,4), low FLOAT(8,4), adj_close FLOAT(8,4), " \
                            f"volume INT, split VARCHAR(25), dividend FLOAT(8,4), ma FLOAT(8,4), ema FLOAT(8,4), stoch FLOAT(8,4), " \
                            f"macd FLOAT(8,4), boll_bands FLOAT(8,4), rsi FLOAT(8,4), fibo_retrac FLOAT(8,4), ichimoku FLOAT(8,4), " \
                            f"std_dev FLOAT(8,4), avg_dir_idx FLOAT(8,4));"

            try:
                conn = connect.connect(
                    host=self.host,
                    user=self.root_user,
                    password=self.root_pass,
                    database=self.stock_db_name
                )
                cursor = self.conn.cursor()
                cursor.execute(sql_statement)
                conn.close()
                utility.printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            except:
                db = database()
                db.insert_error_log(f"ERROR CREATING TABLE PRICES {stock}: {self.conn.get_warnings}")
                print(f"ERROR CREATING TABLE {stock}: {self.conn.get_warnings}")
                result = False
        return result

