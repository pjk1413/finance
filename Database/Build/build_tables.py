from Database.Service.database import database
from Database.Build.build_database import build_database as build
import config_read as config
from Database.Service.database import insert_error_log
from Database.Service.database import insert_status_log
from Database.utility import list_of_schema
import mysql.connector.errors as mysqlError
from Database.Service.database import insert_log_statement
import sys
import subprocess
import Data.Init_Gather.gather_stock_data as gsd


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
        function_list = [
            self.create_error_log_table(),
            self.create_status_table(),
            self.create_stock_list_table(),
            self.prepare_dow_table(),
            gsd.gather_stock_data().run_init(),
            self.create_all_stock_tables(),
            self.create_all_sentiment_tables(),
            self.grant_access_to_stock_tables()
        ]

        try:
            for func in function_list:
                result = True
                result = func
                if not result:
                    print("ERROR : Unable to create all tables.  Application will exit...")
                    insert_error_log("ERROR IN TABLE BUILD - DID NOT COMPLETE BUILD TABLE TASKS - PROGRAM TERMINATED")
            insert_status_log("FINISHED : All tables setup/startup completed successfully")
        except:
            input("PRESS ANY KEY TO EXIT PROGRAM")
            sys.exit(0)

    def create_status_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS STATUS_TBL (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, statement VARCHAR(255));"
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)

            insert_log_statement("Status table created successfully")
            return True
        except mysqlError:
            insert_log_statement(f"ERROR : Could not create Status table \n SQL Error: {mysqlError}")
            return False


    def create_error_log_table(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS error_log (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, description VARCHAR(255));"
        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            insert_log_statement("Error Log table created successfully")
            return True
        except mysqlError:
            insert_log_statement(f"ERROR : Could not create Error Log table \n SQL Error : {mysqlError}")
            return False


    def create_stock_list_table(self):
        table_name = "STOCK_LIST_TBL"
        sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (ticker VARCHAR(50) PRIMARY KEY, name VARCHAR(255), sector VARCHAR(60), industry VARCHAR(60), status VARCHAR(6), country VARCHAR(50), market VARCHAR(25), " \
                        f"currency VARCHAR(50), fullTimeEmployees BIGINT, description VARCHAR(2000), lastSplitDate DATETIME, lastSplitFactor VARCHAR(10), " \
                        f"location VARCHAR(500), website VARCHAR(255), secFilingWebsite VARCHAR(255), lastUpdated DATETIME, dividendDate DATETIME, assetType VARCHAR(15), " \
                        f"exDividendDate DATETIME, payoutRatio FLOAT(12,4), forwardAnnualDividendYield FLOAT(12,4), forwardAnnualDividendRate FLOAT(12,4), " \
                        f"percentInstitutions FLOAT(12,4), percentInsiders FLOAT(12,4), shortPercentFloat FLOAT(12,4), shortPercentOutstanding FLOAT(12,4), " \
                        f"shortRatio FLOAT(12,4), sharesShortPriorMonth BIGINT, sharesShort BIGINT, sharesFloat BIGINT, sharesOutstanding BIGINT, 200MA FLOAT(12,4), " \
                        f"50MA FLOAT(12,4), 52Low FLOAT(12,4), 52High FLOAT(12,4), beta FLOAT(12,4), EVToEBITDA FLOAT(12,4), EVToRevenue FLOAT(12,4), " \
                        f"priceToBookRatio FLOAT(12,4), priceToSalesRatioTTM FLOAT(12,4), forwardPE FLOAT(12,4), trailingPE FLOAT(12,4), analystTargetPrice FLOAT(12,4), " \
                        f"quarterlyRevenueGrowthYOY FLOAT(12,4), quarterlyEarningsGrowthYOY FLOAT(12,4), dilutedEPSTTM FLOAT(12,4), grossProfitTTM BIGINT, " \
                        f"revenueTTM BIGINT, returnOnEquityTTM FLOAT(12,4), returnOnAssestsTTM FLOAT(12,4), operatingMarginTTM FLOAT(12,4), profitMargin FLOAT(12,4), " \
                        f"revenuePerShareTTM FLOAT(12,4), eps FLOAT(12,4), dividendYield FLOAT(12,4), dividendPerShare FLOAT(12,4), bookValue FLOAT(12,4), " \
                        f"pegRatio FLOAT(12,4), peRatio FLOAT(12,4), EBITDA INT, marketCapitalization BIGINT, latestQuarter DATETIME, ipoDate DATETIME, " \
                        f"delistingDate DATETIME, " \
                        f"CONSTRAINT unique_entry UNIQUE (ticker));"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            insert_log_statement("Stock List table created successfully")
            return True
        except mysqlError:
            insert_error_log(f"ERROR : Could not create Stock List table \n SQL Error : {mysqlError}")
            return False

    def create_sentiment_reference_table(self):
        result = True
        sql_statement_sentiment = f"CREATE TABLE IF NOT EXISTS SENTIMENT_DATA_REFERENCE (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                  f"ticker VARCHAR(10), SENTIMENT_ID INT, " \
                                  f"FOREIGN KEY (SENTIMENT_ID) REFERENCES SENTIMENT_DATA(id));"
        try:
            cursor = self.conn_sentiment.cursor()
            cursor.execute(sql_statement_sentiment)
            insert_log_statement("Sentiment_Data reference table created successfully")
        except mysqlError:
            insert_error_log(f"ERROR CREATING TABLE REFERENCE: {mysqlError}")
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
        if not self.create_sentiment_reference_table():
            result = False
        return result


    def create_sentiment_table(self):
        result = True
        sql_statement_sentiment = f"CREATE TABLE IF NOT EXISTS SENTIMENT_DATA (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                  f"crawlDate DATETIME, publishedDate VARCHAR(100), tickers VARCHAR(2000), tags VARCHAR(2000), " \
                                  f"description TEXT, source VARCHAR(255), title VARCHAR(500), url VARCHAR(500), " \
                                  f"sent_neg FLOAT(8,4), sent_neutral FLOAT(8,4), sent_pos FLOAT(8,4), sent_compound FLOAT(8,4), " \
                                  f"CONSTRAINT unique_value UNIQUE (url, publishedDate));"
        try:
            cursor = self.conn_sentiment.cursor()
            cursor.execute(sql_statement_sentiment)
        except mysqlError:
            insert_error_log(f"ERROR CREATING TABLE SENTIMENT: {mysqlError}")
            result = False
        return result


    def create_predict_tables(self):
        # Create all tables with all variables needed to model data appropriatley
        result = True
        sql_statement_linear_regression = "CREATE TABLE IF NOT EXISTS linear_regression (id INT AUTO_INCREMENT PRIMARY KEY, " \
                                          "dt DATETIME, stock VARCHAR(10), description VARCHAR(100), b FLOAT(12,4), x FLOAT(12,4)"

    def create_all_stock_tables(self):
        result = True
        sql_statement = f"CREATE TABLE IF NOT EXISTS STOCK_DATA (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(15), " \
                        f"dt DATETIME NOT NULL, open FLOAT(12,4), close FLOAT(12,4), high FLOAT(12,4), low FLOAT(12,4), adj_close FLOAT(12,4), " \
                        f"volume INT, split VARCHAR(25), dividend FLOAT(12,4), " \
                        f"CONSTRAINT unique_entry UNIQUE (ticker, dt));"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            insert_log_statement("Stock Data table created successfully")
        except mysqlError:
            insert_error_log(f"ERROR CREATING TABLE PRICES: {self.conn.get_warnings}")
            result = False
        return result

    def prepare_dow_table(self):
        # TODO List of dow_30 stocks needs to be placed somewhere to be looped through
        dow_list = []
        sql_statement = f"CREATE TABLE IF NOT EXISTS DOW_30 (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(10));"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            insert_log_statement("Dow30 table created successfully")
            return True
        except:
            insert_error_log(f"ERROR CREATING TABLE DOW_30: {self.conn.get_warnings}")
            return False
