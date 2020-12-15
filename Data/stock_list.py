import requests
import csv
from Data.config_read import config as get_values
import yfinance as yf
import Database.build_tables as tables
from Database.database import database
import mysql.connector as connect
import Database.crud_func as CRUD
import Interface.utility as utility
from Database.database import insert_error_log
import datetime

#########################################
# Gathers a list of stocks to be brought into the database
# Refreshes list on a regular basis
# Creates and manages a directory of files

class stock_list:
    def __init__(self):
        config = get_values()
        self.conn_stock = database().conn_stock
        self.tables = tables.build_tables()
        self.directory = config.file_location
        self.tiingo_api_key = config.tiingo_api_key
        self.alphavantage_api_key = config.alpha_vantage_api_key
        self.stock_table_list_name = "STOCK_LIST_TBL"

    def download_data(self):
        response = requests.get(f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={self.alphavantage_api_key}")
        if response.status_code != 200:
            insert_error_log(f"ERROR: Could not read data from server, status code: {response.status_code}")
            return "Error"
        else:
            open("./stock_listings.csv", "wb").write(response.content)

    # ALPHAVANTAGE - SWITCH TOO
    def update_stock_list(self):
        self.download_data()
        with open('./stock_listings.csv') as file:
            csv_reader = csv.reader(file, delimiter=',')
            for i, row in enumerate(csv_reader):
                if i == 0:
                    print(row)
                else:
                    ticker = row[0]
                    name = row[1]
                    exchange = row[2]
                    assetType = row[3]
                    ipoDate = row[4]
                    delistingDate = row[5]
                    status = row[6]

                    sql_statement = f"REPLACE INTO {self.stock_table_list_name} SET ticker = '{ticker}', name = '{name}', status = '{status}', " \
                                f"exchange = '{exchange}', assetType = '{assetType}', ipoDate = '{ipoDate}', delistingDate = '{delistingDate}';"
                    try:
                        cursor = connect.connect(
                            host=f"{self.host}",
                            user=f"{self.user}",
                            password=f"{self.password}",
                            database=f"{self.stock_db}"
                        ).cursor()
                        cursor.execute(sql_statement)
                    except:
                        print("ERROR: Could not replace stock listings")


    def get_list_of_stocks(self):
        sql_statement = f"SELECT ticker, sector, industry, market FROM STOCK_LIST_TBL;"

        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            # return [('AAPL',), ('GOOG',), ('SPCE',), ('AMZN',), ('MSFT',), ('PYPL',), ('GM',), ('NFLX',)]
            return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn_stock.get_warnings)
            print("ERROR : Could not fetch list of stock symbols")


    # Takes the list of stocks (txt file) and inserts them into the database, searching for additional data using yahoo finance
    def list_to_db(self):
        self.tables.create_stock_list_table()
        file = open(f"./{self.directory}/nasdaq.txt")

        l = len(file.readlines()[1:])
        utility.printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
        for i, line in enumerate(file.readlines()[1:]):
            data = line.split("|")

            if len(data) < 2:
                continue

            if CRUD.crud().check_stock_entry_in_list(data[0]):
                continue

            symbol = yf.Ticker(data[0])
            sector, industry = None, None
            try:
                sector = symbol.info["sector"]
                industry = symbol.info["industry"]
            except:
                pass

            data_dict = {
                "ticker" : data[0],
                "description" : data[1],
                "sector" : sector,
                "industry" : industry
            }

            sql_statement = f"INSERT INTO {self.stock_table_list_name} (ticker, description, sector, industry, market) " \
                        "VALUES (%s, %s, %s, %s, %s)"
            values = (data_dict["ticker"], data_dict["description"], data_dict["sector"], data_dict["industry"], "nasdaq")

            try:
                cursor = self.conn.cursor()
                cursor.execute(sql_statement, values)
                self.conn.commit()
                utility.printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
            except connect.errors as err:
                print(err)
                return False
        return True




