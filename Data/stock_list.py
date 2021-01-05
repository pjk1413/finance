import requests
import csv
from config_read import config as get_values
import Database.Build.build_tables as tables
from Database.Service.database import database
import mysql.connector as connect
from Database.Service.database import insert_error_log
import mysql.connector.errors as err

# Gathers a list of stocks to be brought into the database
# Refreshes list on a regular basis
# Creates and manages a directory of files

# TODO does this need to be a class object, or would it be better as a series of functions
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

    # TODO convert into insert/update statement
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

                    if delistingDate == 'null' or delistingDate == None:
                        delistingDate = '0000-00-00'

                    sql_statement = f"REPLACE INTO {self.stock_table_list_name} SET ticker = '{ticker}', name = '{name}', status = '{status}', " \
                                f"assetType = '{assetType}', ipoDate = '{ipoDate}', delistingDate = '{delistingDate}';"
                    try:
                        # cursor = connect.connect(
                        #     host=f"{self.host}",
                        #     user=f"{self.user}",
                        #     password=f"{self.password}",
                        #     database=f"{self.stock_db}"
                        # ).cursor()
                        cursor = self.conn_stock.cursor()
                        cursor.execute(sql_statement)
                    except err:
                        print("ERROR: Could not replace stock listings")


    def get_list_of_stocks(self, status="active", assetType="stock"):
        sql_statement = f"SELECT ticker, sector, industry, market FROM STOCK_LIST_TBL WHERE " \
                        f"status='{status}' AND assetType='{assetType}';"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn_stock.get_warnings)
            print("ERROR : Could not fetch list of stock symbols")





