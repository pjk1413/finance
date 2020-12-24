import Utility.multithreading as multi_threading
import requests
from Database.database import insert_error_log
import Data.config_read as con
import Database.database as database
import csv
import json
import time


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

    def update_stock_list(self):
        list_statements = []

        start = time.perf_counter()
        self.download_data()
        finish = time.perf_counter()
        print(f"Downloaded stock listings in {finish - start} seconds \n")

        start = time.perf_counter()
        with open('./stock_listings.csv') as file:
            csv_reader = csv.reader(file, delimiter=',')
            for i, row in enumerate(csv_reader):
                if i == 0:
                    pass
                else:
                    ticker = str(row[0]).replace("-","_").replace(" ", "")
                    name = row[1]
                    exchange = row[2]
                    assetType = row[3]
                    ipoDate = '0000-00-00' if row[4] == 'null' else row[4]
                    delistingDate = '0000-00-00' if row[5] == 'null' else row[5]
                    status = row[6]
                    sql_statement = f"INSERT INTO {self.stock_table_list_name} (ticker, name, status, market, assetType, ipoDate, delistingDate) " \
                                    f"VALUES ('{ticker}', '{name}', '{status}', '{exchange}', '{assetType}', '{ipoDate}', '{delistingDate}') " \
                                    f"ON DUPLICATE KEY UPDATE " \
                                    f"name='{name}', status='{status}', market='{exchange}', assetType='{assetType}', " \
                                    f"ipoDate='{ipoDate}', delistingDate='{delistingDate}';"
                    list_statements.append(sql_statement)
        try:
            threader = multi_threading.Multi_Threading(list_statements, "technical")
            threader.run()
            finish = time.perf_counter()
            print(f"Loaded all stock listings in database in {finish - start} seconds")
            return True
        except:
            print("Error using threader")

    def execute_sql(self, sql_statement, thread_id):
        cursor = self.conn_stock.cursor()
        cursor.execute(sql_statement)
        self.conn_stock.commit()

    # @yaspin(text="Gathering extra data...")
    def gather_extra_data(self):
        list = self.get_list_of_stocks()
        for entry in list:
            ticker = entry[0].replace("_", ".")
            response = requests.get(
                f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={self.alphavantage_api_key}")
            if response.status_code != 200:
                insert_error_log(f"ERROR: Could not read data from server, status code: {response.status_code}")
                return "Error"
            else:
                data = json.loads(response.text)

                sector = check(data, 'Sector', 's')
                industry = check(data, 'Industry', 's')
                country = check(data, 'Country', 's')
                currency = check(data, 'Currency', 's')
                fullTimeEmployees = check(data, 'FullTimeEmployees', 'i')
                description = check_long(data, 'Description', 's')
                lastSplitDate = check(data, 'LastSplitDate', 'd')
                lastSplitFactor = check(data, 'LastSplitFactor', 'f')
                location = check(data, 'Address', 's')
                dividendDate = check(data, 'DividendDate', 'd')
                exdividendDate = check(data, 'ExDividendDate', 'd')
                payoutRatio = check(data, 'PayoutRatio', 'f')
                percentInstitutions = check(data, 'PercentInstitutions', 'f')
                percentInsiders = check(data, 'PercentInsiders', 'f')
                forwardAnnualDividendYield = check(data, 'ForwardAnnualDividendYield', 'f')
                forwardAnnualDividendRate = check(data, 'ForwardAnnualDividendRate', 'f')
                shortPercentFloat = check(data, 'ShortPercentFloat', 'f')
                shortPercentOutstanding = check(data, 'ShortPercentOutstanding', 'f')
                shortRatio = check(data, 'ShortRatio', 'f')
                sharesShortPriorMonth = check(data, 'SharesShortPriorMonth', 'i')
                sharesShort = check(data, 'SharesShort', 'i')
                sharesFloat = check(data, 'SharesFloat', 'i')
                sharesOutstanding = check(data, 'SharesOutstanding', 'i')
                MA200 = check(data, '200DayMovingAverage', 'f')
                MA50 = check(data, '50DayMovingAverage', 'f')
                Low52 = check(data, '52WeekLow', 'f')
                High52 = check(data, '52WeekHigh', 'f')
                beta = check(data, 'Beta', 'f')
                evtoebitda = check(data, 'EVToEBITDA', 'f')
                evtorevenue = check(data, 'EVToRevenue', 'f')
                priceToBookRatio = check(data, 'PriceToBookRatio', 'f')
                priceToSalesRatioTTM = check(data, 'PriceToSalesRatioTTM', 'f')
                forwardPE = check(data, 'ForwardPE', 'f')
                trailingPE = check(data, 'TrailingPE', 'f')
                analystTargetPrice = check(data, 'AnalystTargetPrice', 'f')
                quarterylyRevenueGrowthYOY = check(data, 'QuarterlyRevenueGrowthYOY', 'f')
                quarterlyEarningsGrowthYOY = check(data, 'QuarterlyEarningsGrowthYOY', 'f')
                dilutedEPSTTM = check(data, 'DilutedEPSTTM', 'f')
                grossProfitTTM = check(data, 'GrossProfitTTM', 'i')
                revenueTTM = check(data, 'RevenueTTM', 'f')
                returnOnEquityTTM = check(data, 'ReturnOnEquityTTM', 'f')
                returnOnAssetsTTM = check(data, 'ReturnOnAssetsTTM', 'f')
                operatingMarginTTM = check(data, 'OperatingMarginTTM', 'f')
                profitMargin = check(data, 'ProfitMargin', 'f')
                revenuePerShareTTM = check(data, 'RevenuePerShareTTM', 'f')
                eps = check(data, 'EPS', 'f')
                dividendYield = check(data, 'DividendYield', 'f')
                dividendPerShare = check(data, 'DividendPerShare', 'f')
                bookValue = check(data, 'BookValue', 'f')
                pegRatio = check(data, 'PEGRatio', 'f')
                peRatio = check(data, 'PERatio', 'f')
                EBITDA = check(data, 'EBITDA', 'i')
                marketCap = check(data, 'MarketCapitalization', 'i')
                latestQuarter = check(data, 'LatestQuarter', 'd')
                sql_statement = f"UPDATE STOCK_LIST_TBL " \
                                f"SET sector='{sector}', industry='{industry}', country='{country}', currency='{currency}', " \
                                f"fullTimeEmployees='{fullTimeEmployees}', description='{self.cl(description)}', lastSplitDate='{lastSplitDate}', " \
                                f"lastSplitFactor='{lastSplitFactor}', location='{location}', " \
                                f"dividendDate='{dividendDate}', exdividendDate='{exdividendDate}', payoutRatio='{payoutRatio}', " \
                                f"forwardAnnualDividendYield='{forwardAnnualDividendYield}', forwardAnnualDividendRate='{forwardAnnualDividendRate}', " \
                                f"percentInstitutions='{percentInstitutions}', percentInsiders='{percentInsiders}', " \
                                f"shortPercentFloat='{shortPercentFloat}', shortPercentOutstanding='{shortPercentOutstanding}', " \
                                f"shortRatio='{shortRatio}', sharesShortPriorMonth='{sharesShortPriorMonth}', " \
                                f"sharesShort='{sharesShort}', sharesFloat='{sharesFloat}', sharesOutstanding='{sharesOutstanding}', 200MA='{MA200}', " \
                                f"50MA='{MA50}', 52Low='{Low52}', 52High='{High52}', beta='{beta}', EVToEBITDA='{evtoebitda}', EVToRevenue='{evtorevenue}', " \
                                f"priceToBookRatio='{priceToBookRatio}', priceToSalesRatioTTM='{priceToSalesRatioTTM}', forwardPE='{forwardPE}', trailingPE='{trailingPE}', " \
                                f"analystTargetPrice='{analystTargetPrice}', quarterlyRevenueGrowthYOY='{quarterylyRevenueGrowthYOY}', " \
                                f"quarterlyEarningsGrowthYOY='{quarterlyEarningsGrowthYOY}', dilutedEPSTTM='{dilutedEPSTTM}', grossProfitTTM='{grossProfitTTM}', " \
                                f"revenueTTM='{revenueTTM}', returnOnEquityTTM='{returnOnEquityTTM}', returnOnAssestsTTM='{returnOnAssetsTTM}', " \
                                f"operatingMarginTTM='{operatingMarginTTM}', profitMargin='{profitMargin}', revenuePerShareTTM='{revenuePerShareTTM}', " \
                                f"eps='{eps}', dividendYield='{dividendYield}', dividendPerShare='{dividendPerShare}', bookValue='{bookValue}', " \
                                f"pegRatio='{pegRatio}', peRatio='{peRatio}', EBITDA='{EBITDA}', marketCapitalization='{marketCap}', " \
                                f"latestQuarter='{latestQuarter}' " \
                                f"WHERE ticker='{entry[0]}';"
                try:
                    cursor = self.conn_stock.cursor()
                    cursor.execute(sql_statement)
                    self.conn_stock.commit()
                    print(f"Updated data {entry[0]}")
                    time.sleep(30)
                except:
                    database.insert_error_log("ERROR: Failed to update data")


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

    def cl(self, str):
        str = str[:1999]
        str = str.replace("'", "")
        str = str.replace('"', '')
        return str

def check(data, key, type):
    try:
        value = data[key]
        if value == None or value == 'None':
            if type == 'i':
                return -1
            elif type == 'f':
                return -1
            elif type == 's':
                return 'None'
            elif type == 'd':
                return '000-00-00'
        return value
    except:
        if type == 'i':
            return -1
        elif type == 'f':
            return -1
        elif type == 's':
            return 'None'
        elif type == 'd':
            return '000-00-00'

def check_long(data, key, type):
    try:
        value = data[key]
        if len(value) > 1999:
            return value[:1999]
        else:
            return value
    except:
        if type == 's':
            return 'None'