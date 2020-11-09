import mysql.connector as connect
import datetime
import DatabaseManagement.DateHandle as DateH
import finnhub
import pandas as pd

def execute_sql_entry_check(sql):
    conn = connect.connect(
        host="192.168.1.57",
        user="patrick",
        password="12345",
        database="test_database"
    )
    mysql = conn.cursor()
    mysql.execute(sql)
    rows = mysql.fetchall()
    if len(rows) > 0:
        return True
    else:
        return False

def execute_sql_insert(sql, values):
    conn = connect.connect(
        host="192.168.1.57",
        user="patrick",
        password="12345",
        database="test_database"
    )
    mysql = conn.cursor(buffered=True)
    mysql.execute(sql, values)
    conn.commit()


def date_test():
    date = datetime.datetime.now()
    conn = connect.connect(
        host="192.168.1.57",
        user="patrick",
        password="12345",
        database="test_database"
    )

    mysql = conn.cursor()
    mysql.execute("SELECT ticker, dt, high, low, open, close FROM finance_quant.candlestick_60m WHERE ticker='A';")
    data = mysql.fetchall()
    # print(data[0][1])
    new_date = data[0][1]
    print(datetime.datetime.now().time().hour > datetime.datetime.time(data[0][1]).hour)
    print(datetime.datetime.time(data[0][1]).hour)
    # print(datetime.datetime.now())



def create_test_database_schema():
    conn = connect.connect(
        host="192.168.1.57",
        user="patrick",
        password="12345"
    )

    mysql = conn.cursor()
    mysql.execute("CREATE DATABASE test_database;")

    # conn = connect.connect(
    #     host="192.168.1.57",
    #     user="patrick",
    #     password="12345",
    #     database="test_database"
    # )

    # mysql = conn.cursor()
    #
    # mysql.execute(f"CREATE TABLE candlestick_60m_test " \
    #               "(id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(8), market VARCHAR(8), dt DATETIME, open DOUBLE, " \
    #               "close DOUBLE, " \
    #               "high DOUBLE, low DOUBLE, volume INT, dividends DOUBLE, stock_splits DOUBLE);")
    #
    # mysql.execute(f"CREATE TABLE candlestick_1D_test " \
    #               "(id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(8), market VARCHAR(8), dt DATETIME, open DOUBLE, " \
    #               "close DOUBLE, " \
    #               "high DOUBLE, low DOUBLE, volume INT, dividends DOUBLE, stock_splits DOUBLE);")

def test():
    finnhub_client = finnhub.Client("btclgpv48v6rudshgts0")

    # symbol, period, from, to (dates as unix values)
    current_time = datetime.datetime.now()
    stock_data = finnhub_client.stock_candles('A', '60', toUnix(getPastDate(60)), toUnix(current_time))
    data = pd.DataFrame(stock_data)
    row = data.iloc[0,:]
    print(row.loc['s'] == 'ok')

def get_all_days_hours(days_back):
    total_hours = days_back * 24
    delta = datetime.datetime.now() - datetime.timedelta(hours=total_hours)

    date_range = []
    print(total_hours)
    for i in range(total_hours):
        time = delta + datetime.timedelta(hours=i)

        if (time.hour > 12 and time.hour < 20):
            if(time.weekday()):
                date_range.append(time)

    return date_range
        # print(delta + datetime.timedelta(hours=i))
#     13:30-19:30

def get_all_days(start):
    delta = datetime.datetime.now() - start
    for i in range(delta.days + 1):
        print(start + datetime.timedelta(days=i))

def getPastDate(days_back):
    total_hours = days_back * 24
    delta = datetime.datetime.now() - datetime.timedelta(hours=total_hours)
    return delta

def toDateTime(timestamp):
    return datetime.datetime.utcfromtimestamp(float(timestamp))

# converts to utc time automatically
def toUnix(timestamp):
    return timestamp.timestamp()

def update_stock_prices_hour():

    # Connect to database
    conn = connect.connect(
            host="192.168.1.57",
            user="patrick",
            password="12345",
            database="test_database"
            )
    mysql = conn.cursor()


    # Get List of symbols from database
    mysql.execute("SELECT * FROM finance_quant.ticker_symbols WHERE symbol='A';")
    symbol_list = mysql.fetchall()

    # go through each symbol in the list
    for s in symbol_list:
        symbol = s[1]
        # query database for all records pertaining to symbol, market, high, low, open, close, volume?
        mysql = conn.cursor()

        # Gets all the data from the table
        mysql.execute(f"SELECT ticker, dt, high, low, open, close, market FROM test_database.candlestick_60m_test WHERE ticker='{symbol}';")
        symbol_data = mysql.fetchall()
        market = symbol_data[0][6]

        # Goes through each entry for the symbol
        for entry in symbol_data:
            # Grab all dates between current date and last entry that are valid (no holidays or weekends) - use to check for empty spots
            # date_list = get_all_days_hours()

            # Use finnhub to fill in gaps
            finnhub_client = finnhub.Client("btclgpv48v6rudshgts0")

            # symbol, period, from, to (dates as unix values)
            current_time = datetime.datetime.now()
            data = finnhub_client.stock_candles('AAPL', 'D', toUnix(getPastDate(10)), toUnix(current_time))
            print(data)
            stock_data = pd.DataFrame(data)
            count_rows = stock_data.shape[0]

            for i in range(0, count_rows):
                row = stock_data.iloc[i, :]
                status = row.loc['s']

                if status != 'ok':
                    continue

                open = row.loc['o']
                close = row.loc['c']
                high = row.loc['h']
                low = row.loc['l']
                volume = row.loc['v']
                date = toDateTime(row.loc['t'])

                sql_statement = f"INSERT INTO candlestick_60m_test (high, low, open, close, volume, dt, ticker, market) " \
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                values = (float(high), float(low), float(open), float(close), float(volume), date, symbol, market)

                # check for matching data before inserting
                check_sql_statement = f"SELECT dt,ticker FROM test_database.candlestick_60m_test WHERE dt='{date}' AND ticker='{symbol}';"
                if not execute_sql_entry_check(check_sql_statement):
                    execute_sql_insert(sql_statement, values)
                    print(f"Date: {date} - Symbol: {symbol} INSERTED")
                else:
                    print(f"Ticker: {symbol} with date: {date} entry already exists")






