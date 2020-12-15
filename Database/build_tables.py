from Database.database import database
from Database.build_database import build_database as build
import Data.stock_list as stk_list
import Data.config_read as config
from Database.database import insert_error_log
from Database.database import insert_status_log
import Interface.utility as utility
from Database.utility import list_of_schema
import Data.Technical_Data.Model.stock_model as stock_model
import mysql.connector.errors as mysqlError
from Database.database import insert_log_statement
import os
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
        result = True
        result = self.create_error_log_table()
        result = self.create_status_table()
        result = self.create_stock_list_table()
        result = self.create_all_sentiment_tables()
        result = self.prepare_dow_table()

        subprocess.call('start py run.py', shell=True)
        # os.system('cmd /k "py run.py"')
        print("PAST")
        # gsd.gather_stock_data().update_stock_list()
        # result = stk_list.stock_list().update_stock_list()

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
            insert_status_log("FINISHED : All tables setup/startup completed successfully")
        else:
            insert_log_statement("""
                    ERROR : Error during setup/startup of table build \n \n
                    Application will now exit...""")
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
        sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        f"ticker VARCHAR(50), name VARCHAR(255), sector VARCHAR(60), industry VARCHAR(60), status VARCHAR(6), country VARCHAR(50), market VARCHAR(25), " \
                        f"currency VARCHAR(50), fullTimeEmployees INT, description VARCHAR(500), lastSplitDate DATETIME, lastSplitFactor VARCHAR(10), " \
                        f"location VARCHAR(50), website VARCHAR(255), secFilingWebsite VARCHAR(255), lastUpdated DATETIME, dividendDate DATETIME, assetType VARCHAR(15), " \
                        f"exDividendDate DATETIME, payoutRatio FLOAT(8,4), forwardAnnualDividendYield FLOAT(8,4), forwardAnnualDividendRate FLOAT(8,4), " \
                        f"percentInstituions FLOAT(8,4), percentInsiders FLOAT(8,4), shortPercentFloat FLOAT(8,4), shortPercentOutstanding FLOAT(8,4), " \
                        f"shortRatio FLOAT(8,4), sharesShortPriorMonth INT, sharesShort INT, sharesFloat INT, sharesOutstanding INT, 200MA FLOAT(8,4), " \
                        f"50MA FLOAT(8,4), 52Low FLOAT(8,4), 52High FLOAT(8,4), beta FLOAT(8,4), EVToEBITDA FLOAT(8,4), EVToRevenue FLOAT(8,4), " \
                        f"priceToBookRatio FLOAT(8,4), priceToSalesRatioTTM FLOAT(8,4), forwardPE FLOAT(8,4), trailingPE FLOAT(8,4), analystTargetPrice FLOAT(8,4), " \
                        f"quarterlyRevenueGrowthYOY FLOAT(8,4), quarterlyEarningsGrowthYOY FLOAT(8,4), dilutedEPSTTM FLOAT(8,4), grossProfitTTM INT, " \
                        f"revenueTTM INT, returnOnEquityTTM FLOAT(8,4), returnOnAssestsTTM FLOAT(8,4), operatingMarginTTM FLOAT(8,4), profitMargin FLOAT(8,4), " \
                        f"revenuePerShareTTM FLOAT(8,4), eps FLOAT(8,4), dividendYield FLOAT(8,4), dividendPerShare FLOAT(8,4), bookValue FLOAT(8,4), " \
                        f"pegRatio FLOAT(8,4), peRatio FLOAT(8,4), EBITDA INT, marketCapitalization INT, latestQuarter DATETIME, ipoDate DATETIME, " \
                        f"delistingDate DATETIME);"
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
                                  f"ticker VARCHAR(10), SENTIMENT_ID INT);"
        try:
            cursor = self.conn_sentiment.cursor()
            cursor.execute(sql_statement_sentiment)
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
                                  f"crawlDate DATETIME, publishedDate VARCHAR(500), tickers VARCHAR(1000), tags VARCHAR(1000), " \
                                  f"description TEXT, source VARCHAR(255), title VARCHAR(500), url VARCHAR(500), " \
                                  f"sent_neg FLOAT(8,4), sent_neutral FLOAT(8,4), sent_pos FLOAT(8,4), sent_compound FLOAT(8,4));"
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
                                          "dt DATETIME, stock VARCHAR(10), description VARCHAR(100), b FLOAT(8,4), x FLOAT(8,4)"


    def create_all_stock_tables(self):
        result = True
        stock_list = stk_list.stock_list().get_list_of_stocks()
        list_of_indicators = stock_model.stock_model().get_model_indicators()

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
                result = False
        return result


    def prepare_dow_table(self):
        # TODO List of dow_30 stocks needs to be placed somewhere to be looped through
        dow_list = []
        sql_statement = f"CREATE TABLE IF NOT EXISTS DOW_30 (id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(10));"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
        except:
            insert_error_log(f"ERROR CREATING TABLE DOW_30: {self.conn.get_warnings}")
