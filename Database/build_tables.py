from Database.database import database
from Database.build_database import build_database as build
import Data.stock_list as stk_list
import Data.config_read as config
from Database.database import insert_error_log
import Interface.utility as utility
from Database.utility import list_of_schema
import Data.Technical_Data.Model.stock_model as stock_model
import mysql.connector.errors as mysqlError
import sys


class build_tables:
    def __init__(self):
        con = config.config()
        self.conn_utility = database().conn_utility
        self.conn_stock = database().conn_stock
        self.conn_sentiment = database().conn_sentiment
        self.host = con.db_host
        self.root_user = con.db_user_root
        self.root_pass = con.db_pass_root
        self.db_user = con.db_user
        self.db_pass = con.db_pass
        self.sentiment_db_name = con.sentiment_db_name
        self.stock_db_name = con.stock_db_name

    def build_tables(self):
        result = True
        result = self.create_error_log_table()
        result = self.create_status_table()
        result = self.create_stock_list_table()
        result = self.create_reference_table()
        result = self.create_all_sentiment_tables()
        result = stk_list.stock_list().list_to_db()

        if result == False:
            print("ERROR : Unable to create all tables.  Application will exit...")
            insert_error_log("ERROR IN TABLE BUILD - DID NOT COMPLETE BUILD TABLE TASKS - PROGRAM TERMINATED")
            input("PRESS ANY KEY TO EXIT PROGRAM")
            sys.exit(0)
        else:
            result = self.create_all_stock_tables()
            result = self.create_all_sentiment_tables()
            result = self.grant_access_to_stock_tables()

        if result == True:
            print("FINISHED : All tables setup/startup completed successfully")
        else:
            print("""
                    ERROR : Error during setup/startup of table build \n \n
                    Application will now exit...""")
            sys.exit(0)


    def create_status_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS STATUS_TBL (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, statement VARCHAR(255));"
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            print("Status table created successfully")
            return True
        except mysqlError:
            print(f"ERROR : Could not create Status table \n SQL Error: {mysqlError}")
            return False


    def create_error_log_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS error_log (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, description VARCHAR(255));"

        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            print("Error Log table created successfully")
            return True
        except mysqlError:
            print(f"ERROR : Could not create Error Log table \n SQL Error : {mysqlError}")
            return False


    def create_stock_list_table(self):
        table_name = "STOCK_LIST_TBL"
        sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        f"ticker VARCHAR(10), description VARCHAR(500), sector VARCHAR(60), industry VARCHAR(60), market VARCHAR(10));"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            print("Stock List table created successfully")
            return True
        except mysqlError:
            insert_error_log(f"ERROR : Could not create Stock List table \n SQL Error : {mysqlError}")
            return False


    def create_reference_table(self):
        result = True
        sql_statement_sentiment = f"CREATE TABLE IF NOT EXISTS DATA_REFERENCE (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                  f"ticker VARCHAR(10), SENTIMENT_ID INT);"
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement_sentiment)
        except mysqlError:
            insert_error_log(f"ERROR CREATING TABLE REFERENCE: {mysqlError}")
            print(f"ERROR CREATING TABLE REFERENCE: {mysqlError}")
            result = False
        return result

    def grant_access_to_stock_tables(self):
        try:
            for db in list_of_schema():
                build().grant_all_user(db)
            return True
        except:
            return False


    def create_all_sentiment_tables(self):
        result = True
        if not self.create_sentiment_table():
            result = False
        return result


    def create_sentiment_table(self):
        result = True
        sql_statement_sentiment = f"CREATE TABLE IF NOT EXISTS SENTIMENT_DATA (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                  f"crawlDate DATETIME, publishedDate VARCHAR(500), tickers VARCHAR(500), tags VARCHAR(500), " \
                                  f"description TEXT, source VARCHAR(255), title VARCHAR(500), url VARCHAR(500), " \
                                  f"sent_neg FLOAT(8,4), sent_neutral FLOAT(8,4), sent_pos FLOAT(8,4), sent_compound FLOAT(8,4));"
        try:
            cursor = self.conn_sentiment.cursor()
            cursor.execute(sql_statement_sentiment)
        except mysqlError:
            insert_error_log(f"ERROR CREATING TABLE SENTIMENT: {mysqlError}")
            print(f"ERROR CREATING TABLE SENTIMENT: {mysqlError}")
            result = False
        return result


    def create_predict_tables(self):
        # Create all tables with all variables needed to model data appropriatley
        result = True
        sql_statement_linear_regression = "CREATE TABLE IF NOT EXISTS linear_regression (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                          "dt DATETIME, stock VARCHAR(10), description VARCHAR(100), b FLOAT(8,4), x FLOAT(8,4)"


    def create_all_stock_tables(self):
        result = True
        stock_list = stk_list.stock_list().get_list_of_stocks()
        list_of_indicators = stock_model.stock_daily().get_model_indicators()

        l = len(stock_list)
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for i, stock in enumerate(stock_list):

            sql_statement = f"CREATE TABLE IF NOT EXISTS {stock[0]}_STK (id INT AUTO_INCREMENT PRIMARY KEY, " \
                            f"dt DATETIME, open FLOAT(10,4), close FLOAT(10,4), high FLOAT(10,4), low FLOAT(10,4), adj_close FLOAT(10,4), " \
                            f"volume INT, split VARCHAR(25), dividend FLOAT(10,4), "

            for ind in list_of_indicators:
                sql_statement += f"{ind} VARCHAR(50)"
                if not list_of_indicators.index(ind) == len(list_of_indicators) - 1:
                    sql_statement += ", "
                else:
                    sql_statement += ");"
            try:
                cursor = self.conn_stock.cursor()
                cursor.execute(sql_statement)
                utility.printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            except mysqlError:
                insert_error_log(f"ERROR CREATING TABLE PRICES {stock}: {self.conn.get_warnings}")
                print(f"ERROR : Could not create table for {stock} \n SQL Error : {mysqlError}")
                result = False
        return result

