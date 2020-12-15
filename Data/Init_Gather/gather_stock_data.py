import requests
from Database.database import insert_error_log
import Data.config_read as con
import Database.database as database
import csv
import mysql.connector as connect

# sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, " \
#                         f"ticker VARCHAR(10), name VARCHAR(50), sector VARCHAR(60), industry VARCHAR(60), status VARCHAR(6), country VARCHAR(50), market VARCHAR(25), " \
#                         f"currency VARCHAR(50), fullTimeEmployees INT, description VARCHAR(500), lastSplitDate DATETIME, lastSplitFactor VARCHAR(10), " \
#                         f"location VARCHAR(50), website VARCHAR(255), secFilingWebsite VARCHAR(255), lastUpdated DATETIME), dividendDate DATETIME, " \
#                         f"exDividendDate DATETIME, payoutRatio FLOAT(8,4), forwardAnnualDividendYield FLOAT(8,4), forwardAnnualDividendRate FLOAT(8,4), " \
#                         f"percentInstituions FLOAT(8, 4), percentInsiders FLOAT(8,4), shortPercentFloat FLOAT(8,4), shortPercentOutstanding FLOAT(8,4), " \
#                         f"shortRatio FLOAT(8,4), sharesShortPriorMonth INT, sharesShort INT, sharesFloat INT, sharesOutstanding INT, 200MA FLOAT(8,4), " \
#                         f"50MA FLOAT(8,4), 52Low FLOAT(8,4), 52High FLOAT(8,4), beta FLOAT(8,4), EVToEBITDA FLOAT(8,4), EVToRevenue FLOAT(8,4), " \
#                         f"priceToBookRatio FLOAT(8,4), priceToSalesRatioTTM FLOAT(8,4), forwardPE FLOAT(8,4), trailingPE FLOAT(8,4), analystTargetPrice FLOAT(8,4), " \
#                         f"quarterlyRevenueGrowthYOY FLOAT(8,4), quarterlyEarningsGrowthYOY FLOAT(8,4), dilutedEPSTTM FLOAT(8,4), grossProfitTTM INT, " \
#                         f"revenueTTM INT, returnOnEquityTTM FLOAT(8,4), returnOnAssestsTTM FLOAT(8,4), operatingMarginTTM FLOAT(8,4), profitMargin FLOAT(8,4), " \
#                         f"revenuePerShareTTM FLOAT(8,4), eps FLOAT(8,4), dividendYield FLOAT(8,4), dividendPerShare FLOAT(8,4), bookValue FLOAT(8,4), " \
#                         f"pegRatio FLOAT(8,4), peRatio FLOAT(8,4), EBITDA INT, marketCapitalization INT, latestQuarter DATETIME;"

class gather_stock_data:
    def __init__(self):
        config = con.config()
        self.alphavantage_api_key = config.alpha_vantage_api_key
        self.stock_table_list_name = 'STOCK_LIST_TBL'
        self.conn_stock = database.database().conn_stock

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
                    print("Loading list of stocks into database...")
                else:

                    ticker = str(row[0]).replace("-",".")
                    name = row[1]
                    exchange = row[2]
                    assetType = row[3]
                    ipoDate = '0000-00-00' if row[4] == 'null' else row[4]
                    delistingDate = '0000-00-00' if row[5] == 'null' else row[5]
                    status = row[6]
                    print(ticker)
                    sql_statement = f"REPLACE INTO {self.stock_table_list_name} SET ticker = '{ticker}', name = '{name}', status = '{status}', " \
                              f"market = '{exchange}', assetType = '{assetType}', ipoDate = '{ipoDate}', delistingDate = '{delistingDate}';"
                    print(sql_statement)
                    try:
                        cursor = self.conn_stock.cursor()
                        cursor.execute(sql_statement)
                        self.conn_stock.commit()
                    except connect.errors:
                      print("ERROR: Could not replace stock listings")

    def gather_extra_data(self):
        list = self.get_list_of_stocks()
        for entry in list:
            # response = requests.get(
            #     f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={self.alphavantage_api_key}")
            # if response.status_code != 200:
            #     insert_error_log(f"ERROR: Could not read data from server, status code: {response.status_code}")
            #     return "Error"
            # else:
            #     open("./stock_listings.csv", "wb").write(response.content)
            print(entry)


    def get_list_of_stocks(self):
        sql_statement = f"SELECT ticker FROM STOCK_LIST_TBL;"

        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn_stock.get_warnings)
            print("ERROR : Could not fetch list of stock symbols")