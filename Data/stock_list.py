from Data.config_read import config as get_values
import os
import wget
from shutil import rmtree as delete_directory
import yfinance as yf
import Database.build_tables as tables
from Database.database import database
import mysql.connector as connect
import Database.crud_func as CRUD
import sys
import Interface.utility as utility

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
        self.stock_table_list_name = "STOCK_LIST_TBL"
        self.exchange = {
            "nasdaq.txt" : config.nasdaq_listed_url,
            "nyse.csv" : config.nyse_listed_url,
            "amex.csv" : config.amex_listed_url
        }

    def get_list_of_stocks(self):
        sql_statement = f"SELECT ticker, sector, industry, market FROM STOCK_LIST_TBL;"

        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            return [('AAPL',), ('GOOG',), ('SPCE',), ('AMZN',), ('MSFT',), ('PYPL',), ('GM',), ('NFLX',)]
            # return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn_stock.get_warnings)
            print("ERROR : Could not fetch list of stock symbols")

    def refresh_directory(self):
        try:
            delete_directory(self.directory)
            os.mkdir(self.directory)
        except:
            print("Failed to refresh directory")


    def create_directory(self):
        try:
            os.mkdir(self.directory)
        except:
            print("Directory already created")


    # dont use
    def download_listings(self):
        results = None

        for ex in self.exchange:
            try:
                print("Beginning Download...")
                results = wget.download(self.exchange[ex], f"./{self.directory}/{ex}", bar=self.bar_progress)

            except:
                self.error.insert_error_log(f"Error downloading stock list {ex}: {results}")
                print(f"Error downloading stock listings: {ex}")


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




