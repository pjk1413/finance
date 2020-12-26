import csv
from json import JSONDecodeError
import mysql.connector as connect
from Interface.utility import printProgressBar
from config_read import config as con
import Data.Technical_Data.Model.stock_model as stock_model
from Data.Technical_Data.Model.stock_model import stock_model
import requests
from Data.stock_list import stock_list
from Database.Service.database import insert_error_log
import datetime
import json
from multiprocessing.pool import ThreadPool
import multiprocessing
from time import time as timer

class retrieve_technical_bulk:
    def __init__(self):
        config = con()
        self.np = config.process_number
        self.tiingo_api_key = config.tiingo_api_key
        self.list_of_stocks = stock_list().get_list_of_stocks()
        self.temp_csv_file_name = "temp_data_load.csv"
        self.size = 0
        self.cores = multiprocessing.cpu_count()

    def run_data_load(self, range='latest'):

        url_list = []
        # Create temp csv file
        with open(self.temp_csv_file_name, mode='w', newline='') as data_download:
            data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            data_writer.writerow(['date', 'ticker', 'open', 'close', 'high', 'low', 'adj_close',
                                    'volume', 'split', 'dividend'])

        # Create list of urls to be used for threading
        start = timer()
        for i, entry in enumerate(self.list_of_stocks):
            url_list.append((entry[0], self.build_url(entry[0], range)))
        print(f"Create URL List: {timer() - start}")
        print(f"Cores Used: {self.cores}")
        print(f"Number of listings: {len(self.list_of_stocks)}")

        # Download all data into csv table
        try:
            results = ThreadPool(self.cores - 1).imap_unordered(self.retrieve_data, url_list)
            l = len(self.list_of_stocks)
            printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)
            for i, path in enumerate(results):
                path
                printProgressBar(i + 1, l, prefix='Progress:', suffix='Complete', length=50)
        except IOError as e:
            print(e)
            print("ERROR")
        except:
            print("ERROR")
        print(f"Download and File Write: {timer() - start}")
        timer()
        # Load data into database
        self.bulk_load_data()
        print(f"Load data into database: {timer() - start}")

    def build_url(self, ticker, range, years_back = 2):
        if range == "historical":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=365 * years_back)).strftime('%Y-%m-%d')
        elif range == "latest":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        elif range == "month":
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        url = f"https://api.tiingo.com/tiingo/daily/{ticker.replace('_','-')}/prices?startDate={start_date}&format=json&resampleFreq=daily&token={self.tiingo_api_key}"
        return url

    def to_csv(self, obj_list):
        with open(self.temp_csv_file_name, mode='a', newline='') as data_download:
            data_writer = csv.writer(data_download, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for x in obj_list:
                data_writer.writerow([x.date, x.symbol, x.open, x.close, x.high, x.low, x.adj_close, x.volume, x.split, x.dividend])

    # TODO evaluate data prior to being parsed
    def parse_stock_obj(self, ticker, data):
        obj_list = []
        if "Error" not in data:
            try:
                for stock_json in data:
                    stock_obj = stock_model()
                    stock_obj.date = datetime.datetime.strptime(stock_json['date'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    stock_obj.open = stock_json['open']
                    stock_obj.close = stock_json['close']
                    stock_obj.high = stock_json['high']
                    stock_obj.low = stock_json['low']
                    stock_obj.adj_close = stock_json['adjClose']
                    stock_obj.volume = stock_json['volume']
                    stock_obj.symbol = ticker
                    stock_obj.split = stock_json['splitFactor']
                    stock_obj.dividend = stock_json['divCash']
                    obj_list.append(stock_obj)
                return obj_list
            except:
                print(ticker)
                print(data)
                insert_error_log("ERROR: Could not parse stock object during data load")
        else:
            return

    # TODO build into class and calculate the overall size of the download
    def retrieve_data(self, url_data):
        ticker = url_data[0]
        url = url_data[1]
        try:
            response = requests.get(url)
            self.size += float(response.headers['content-length'])/1000000
            if "Error" not in response.text:
                try:
                    # TODO check if the data is ok before loading into the parse stock obj
                    self.to_csv(self.parse_stock_obj(ticker, json.loads(response.text)))
                except JSONDecodeError:
                    print(f"ERROR {ticker}")
                    pass
        except requests.exceptions.RequestException:
            print(f"ERROR {ticker}")
            pass

    def bulk_load_data(self):
        config = con()
        try:
            conn_admin = connect.connect(
                host=f"{config.db_host}",
                user=f"{config.db_user_root}",
                password=f"{config.db_pass_root}"
            )
            conn = connect.connect(
                host=f"{config.db_host}",
                user=f"{config.db_user}",
                password=f"{config.db_pass}",
                database=f"{config.stock_db_name}",
                allow_local_infile=True
            )

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = ON;")
            cursor.close()

            # Create temporary table like the table being inserted into
            cursor = conn.cursor()
            cursor.execute(f"CREATE TEMPORARY TABLE temporary_technical_data_table LIKE stock_technical_data.stock_data;")
            cursor.close()

            # Load data into temporary table
            cursor = conn.cursor()
            cursor.execute(f"""LOAD DATA LOCAL INFILE '{self.temp_csv_file_name}' 
                INTO TABLE temporary_technical_data_table 
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS;""")
            cursor.close()

            # Load data into proper table
            cursor = conn.cursor()
            cursor.execute(f"""INSERT INTO stock_technical_data.stock_data
                SELECT * FROM temporary_technical_data_table
                ON DUPLICATE KEY UPDATE ticker = VALUES(ticker), dt = VALUES(dt), open = VALUES(open), close = VALUES(close), high = VALUES(high), 
                low = VALUES(low), adj_close = VALUES(adj_close), volume = VALUES(volume), split = VALUES(split), dividend = VALUES(dividend);""")
            cursor.close()

            # Drop temporary table
            cursor = conn.cursor()
            cursor.execute("DROP TEMPORARY TABLE temporary_technical_data_table;")
            cursor.close()

            cursor = conn_admin.cursor()
            cursor.execute(f"SET GLOBAL local_infile = OFF;")
            cursor.close()
            conn.close()
            conn_admin.close()
        except:
            print("ERROR")