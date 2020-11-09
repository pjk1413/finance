import mysql.connector as connect
import datetime
import mysql

class mysql_db:
    """
    Endpoints are hard coded into this class, to change configuration, change the endpoints here
    * To add in additional databases, add another connection instance in the init
    """

    def __init__(self):
        self.conn_finance = connect.connect(
            host="192.168.1.57",
            user="patrick",
            password="12345",
            database="finance"
        )
        self.database_status = self.build_database()
        self.table_status = self.build_tables()


    def build_database(self):
        sql_statement = f"CREATE DATABASE IF NOT EXISTS {self.conn_finance.database};"
        cursor = self.conn_finance.cursor()
        cursor.execute(sql_statement)

        if self.conn_finance.is_connected():
            print(f"Connected to {self.conn_finance.database}")
            return True
        else:
            print(f"Error connecting to {self.conn_finance.database}")
            return False


    def build_tables(self):
        status = False
        sql_statement = "CREATE TABLE IF NOT EXISTS ticker_symbols (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "symbol VARCHAR(8), description VARCHAR(60), market VARCHAR(6));"
        cursor = self.conn_finance.cursor()
        cursor.execute(sql_statement)

        if not self.conn_finance.get_warnings:
            # print("Table 'ticker_symbols' created succesfully or already exists")
            status = True
        else:
            print("Error creating table ticker_symbols")
            status = False

        sql_statement = "CREATE TABLE IF NOT EXISTS error_log (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        "dt DATETIME, description VARCHAR(150));"
        cursor = self.conn_finance.cursor()
        cursor.execute(sql_statement)

        if not self.conn_finance.get_warnings:
            # print("Table 'error_log' created succesfully or already exists")
            status = True
        else:
            print("Error creating table error_log")
            status = False

        sql_statement = f"CREATE TABLE IF NOT EXISTS technical_indicators (id INT AUTO_INCREMENT PRIMARY KEY, " \
                        f"dt DATETIME, symbol VARCHAR(8), market VARCHAR(8), ma5 FLOAT(8,4), ma10"



        candlestick_tables = ["candlestick_1D"]

        for table_name in candlestick_tables:
            sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY," \
                        "symbol VARCHAR(8), market VARCHAR(8), dt DATETIME, " \
                        "open FLOAT(8,4), close FLOAT(8,4), high FLOAT(8,4), low FLOAT(8,4), volume INT); "
            try:
                cursor = self.conn_finance.cursor()
                cursor.execute(sql_statement)

                if not self.conn_finance.get_warnings:
                    # print(f"Table '{table_name}' created succesfully or already exists")
                    if status:
                        return True
                else:
                    print(f"Error creating table '{table_name}'")
                    return False
            except:
                self.log_error(f"Error creating table {table_name} at {datetime.datetime.now()}")

    def insert_ticker_symbols(self):

        # Paths for the text files storing the data
        paths = ["C:\\Users\\pjk14\\Downloads\\NYSE.txt", "C:\\Users\\pjk14\\Downloads\\NASDAQ.txt"]

        # Loop that inserts and assembles all of the sql scripts
        for path in paths:
            file = open(path, 'r')
            market = file.name.split('\\')
            Lines = file.readlines()

            market_name = market[len(market) - 1].split('.')[0]

            count = 0
            for line in Lines:
                count += 1
                ticker_symbol = line.split("\t")[0]
                description = line.split("\t")[1]

                check_sql_statement = f"SELECT EXISTS(SELECT 1 FROM ticker_symbols WHERE symbol LIKE '{ticker_symbol}' AND market LIKE '{market_name}' LIMIT 1);"
                cursor = self.conn_finance.cursor()
                cursor.execute(check_sql_statement)
                result = cursor.fetchone()
                if (result[0] == 0):
                    try:
                        sql = "INSERT INTO ticker_symbols (symbol, description, market) VALUES (%s, %s, %s)"
                        val = (ticker_symbol, description, market_name)

                        cursor = self.conn_finance.cursor()
                        cursor.execute(sql, val)
                        self.conn_finance.commit()
                    except:
                        self.log_error(f"Error inserting ticker_symbol data into table")
                else:
                    print("Already exists")

            print("Ticker symbols updated")

    def get_table_rows(self, table_name):
        """
        Returns number of rows for a table
        :param table_name:
        :return:
        """
        sql_statement = f"SHOW TABLES LIKE '{table_name}';"
        cursor = self.conn_finance.cursor()
        cursor.execute(sql_statement)
        if cursor.fetchone() == None:
            return None
        else:
            sql_statement = f"SELECT COUNT(*) FROM {self.conn_finance.database}.{table_name};"
            cursor = self.conn_finance.cursor()
            try:
                cursor.execute(sql_statement)
            except:
                self.log_error(f"Error finding table {table_name}")
            return cursor.fetchone()[0]

    def get_symbol(self, symbol):
        """
        Returns the symbol that is passed in to check if it exists
        :return: a dictionary where result index 0 is the status, 1 is the symbol, 2 is the market
        """
        sql_statement = f"SELECT symbol, market FROM finance_quant.ticker_symbols WHERE symbol='{symbol}';"
        cursor = self.conn_finance.cursor()
        try:
            cursor.execute(sql_statement)
        except:
            self.log_error(f"Error finding symbol {symbol} from the database at {datetime.datetime.now()}")
        symbol_data = cursor.fetchone()

        if symbol_data == None:
            status = False
        else:
            status = True

        symbol_dict = {
            "Status" : status,
            "Symbol" : symbol_data[0],
            "Market" : symbol_data[1]
        }

        return symbol_dict


    def get_date_range(self, symbol):
        """
        Returns a list of all dates that should be filled with data
        :param:
        :return:
        """
        sql_statement = f"SELECT dt FROM candlestick_1D WHERE symbol='{symbol}' ORDER BY dt DESC LIMIT 1;"

        cursor = self.conn_finance.cursor()
        cursor.execute()

        current_date = datetime.datetime.now()
        recent_date = cursor.fetchone() #Grab most recent date on the table and compare with current date
        print(recent_date)
        number_of_days = current_date - datetime.datetime.strptime(recent_date, '%Y-%m-%d %H:%M:%S')

        for day in number_of_days.day:
            print(day)



    def insert_data_candlestick(self, data):
        """
        Inserts data into the database
        :param data: array of dictionaries corresponding to all of the data to be inserted into the database
        :return: status
        """
        for entry in data:

            # Check that the entry doesn't exist
            sql_statement = f"SELECT * FROM candlestick_1D WHERE symbol='{entry['Symbol']}' AND dt='{entry['Date']}';"
            cursor = self.conn_finance.cursor()
            cursor.execute(sql_statement)
            result = cursor.fetchone()

            if result == None:

                sql_statement = f"INSERT INTO candlestick_1D (high, low, open, close, volume, dt, symbol, market) " \
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = (entry['High'], entry['Low'], entry['Open'], entry['Close'], entry['Volume'],
                          entry['Date'], entry['Symbol'], entry['Market'])

                try:
                    cursor = self.conn_finance.cursor()
                    cursor.execute(sql_statement, values)
                    self.conn_finance.commit()
                except mysql.connector.errors as er:
                    print(er)
                    self.log_error(f"Error inserting {entry['Symbol']} into candlestick_1D at {datetime.datetime.now()}")



    def log_error(self, statement):
        sql_statement = "INSERT INTO error_log (dt, description) " \
                        "VALUES (%s, %s)"
        values = (datetime.datetime.now(), statement)
        cursor = self.conn_finance.cursor()
        cursor.execute(sql_statement, values)
        self.conn_finance.commit()