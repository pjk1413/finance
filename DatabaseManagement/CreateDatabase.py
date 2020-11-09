from DatabaseManagement.Connection import connect_mysql as mysql
from DataLoad.DataYahoo import candlestick
import math
from datetime import datetime
import DatabaseManagement.DateHandle as dh


# Function for starting from scratch
def create_baseline():
    create_finance_quant()
    insert_ticker_symbols()
    insert_historical_data_finance_quant()

def create_finance_quant():
    conn = mysql("192.168.1.57", "patrick", "12345")
    conn.connect_to_service()
    conn.create_database("finance_quant")

# Not currently being used
# def create_candlestick_table():
#     conn = mysql("192.168.1.20", "username", "12345")
#     conn.connect_to_db("finance_quant")
#
#     sql_statement = "CREATE TABLE candlestick " \
#                     "(id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(8), market VARCHAR(8), dt DATETIME, " \
#                     "open (8,4), close (8,4), high (8,4), low (8,4), volume INT, " \
#                     "dividends (8,4), stock_splits (8,4))"
#
#     conn.execute_sql_statement(sql_statement)

# Inserts ticker symbols into their own table for use when referencing and searching
def insert_ticker_symbols():
    conn = mysql("192.168.1.57", "patrick", "12345")
    conn.connect_to_db("finance_quant")

    # Check if table already exists
    if (not check_table_exists(conn, "ticker_symbols")):
        conn.execute_sql_statement(
            "CREATE TABLE ticker_symbols (id INT AUTO_INCREMENT PRIMARY KEY, symbol VARCHAR(8), description VARCHAR(60), "
            "market VARCHAR(6))")
        print("Table Created")

    # Paths for the text files storing the data
    paths = ["C:\\Users\\pjk14\\Downloads\\NYSE.txt", "C:\\Users\\pjk14\\Downloads\\NASDAQ.txt"]

    # Loop that inserts and assembles all of the sql scripts
    for path in paths:
        file = open(path, 'r')
        market = file.name.split('\\')
        Lines = file.readlines()

        market_name = market[len(market)-1].split('.')[0]

        count = 0
        for line in Lines:
            count += 1
            ticker_symbol = line.split("\t")[0]
            description = line.split("\t")[1]

            check_sql_statement = f"SELECT symbol,market FROM finance_quant.ticker_symbols WHERE symbol='{ticker_symbol}' AND market='{market_name}';"

            if (not conn.execute_sql_entry_check(check_sql_statement)):
                sql = "INSERT INTO ticker_symbols (symbol, description, market) VALUES (%s, %s, %s)"
                val = (ticker_symbol, description, market_name)
                conn.execute_sql_statement_insert(sql, val)
            else:
                pass
                # print(f"Ticker Symbol: {ticker_symbol} in Market: {market_name} entry already exists")

# Checks whether a table exists in the database finance_quant
def check_table_exists(conn, table_name):
    result = True
    sql_statement = f"SELECT * FROM finance_quant.{table_name};"
    try:
        conn.execute_sql_statement(sql_statement)
        # print("Table already exists")
    except:
        result = False

    return result;

# Inserts historical data from yahoo finance
def insert_historical_data_finance_quant():
    conn = mysql("192.168.1.57", "patrick", "12345")
    conn.connect_to_db("test_database")

    sql_statement = "SELECT symbol,market FROM finance_quant.ticker_symbols WHERE symbol='A';";

    symbol_list = conn.execute_sql_statement_select(sql_statement)

    # Creates a table for candlestick data with name 1D for daily stock data
    if (not check_table_exists(conn, "candlestick_1D_test")):
        conn.create_table_candlestick("candlestick_1D_test")
        print("Table Created")

    if (not check_table_exists(conn, "candlestick_60m_test")):
        conn.create_table_candlestick("candlestick_60m_test")
        print("Table Created")

    # Inside this loop, grab all the data, insert into database, into correct table (need seperate table for each time series)
    for s in symbol_list:
        symbol = s[0]

        market = s[1]

        # 1 Day Setup
        stock_history_day = candlestick.get_historical_data_10y(symbol)
        #60 min setup
        stock_history_hour = candlestick.get_historical_data_hour(symbol)

        stock_history = [stock_history_day, stock_history_hour]
        table_list = ["candlestick_1D_test", "candlestick_60m_test"]

        for i in range(2):

            # Gets the numbers of rows of current grab of data
            count_row = stock_history[i].shape[0]

            print(f"Table: 1D ------- {symbol} ------- Rows: {count_row} \n")

            # Goes through each row, cleans the data and inserts it into a mysql database
            for x in range(0, count_row):
                # Get current row of data
                row = stock_history[i].iloc[x, :]

                # Check if data is bad
                if math.isnan(row.loc['High']) or math.isnan(row.loc['Open']) or math.isnan(row.loc['Low']) or math.isnan(row.loc['Close']):
                    continue

                #datetime object
                date = dh.convert_to_datetime(stock_history[i].index.values[x])
                date.strftime('%Y-%m-%d %H:%M:%S')

                high = row.loc['High'].item()
                low = row.loc['Low'].item()
                open = row.loc['Open'].item()
                close = row.loc['Close'].item()
                volume = row.loc['Volume'].item()
                dividends = row.loc['Dividends'].item()
                stock_splits = row.loc['Stock Splits'].item()

                sql_statement = f"INSERT INTO {table_list[i]} (high, low, open, close, volume, dividends, stock_splits, dt, ticker, market) " \
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (float(high), float(low), float(open), float(close), float(volume), float(dividends),
                          float(stock_splits), date, symbol, market)

                #check for matching data before inserting
                check_sql_statement = f"SELECT dt,ticker FROM test_database.{table_list[i]} WHERE dt='{date}' AND ticker='{symbol}';"
                if not conn.execute_sql_entry_check(check_sql_statement):
                    conn.execute_sql_statement_insert(sql_statement, values)
                else:
                    print(f"Ticker: {symbol} with date: {date} entry already exists")


