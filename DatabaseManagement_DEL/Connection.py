import mysql.connector
from mysql.connector import errorcode

class connect_mysql:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.conn = None

    def get_connection(self):
        return self.conn

    def connect_to_service(self):
        self.conn = mysql.connector.connect(
            host="192.168.1.57",
            user="patrick",
            password="12345",
            )

    def connect_to_db(self, database_name):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=database_name
            )
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def execute_sql_statement_select(self, sql):
        try:
            mysql = self.conn.cursor()
            mysql.execute(sql)
            return mysql.fetchall()
        except:
            pass

    # Checks whether or not the entry already exists
    def execute_sql_entry_check(self, sql):
        mysql = self.conn.cursor()
        mysql.execute(sql)
        rows = mysql.fetchall()
        if len(rows) > 0:
            return True
        else:
            return False

    def execute_sql_statement_insert(self, sql, values):
        mysql = self.conn.cursor(buffered = True)
        mysql.execute(sql, values)
        self.conn.commit()

    def execute_sql_statement(self, statement):
        c = self.conn.cursor(buffered = True)
        c.execute(statement)

    def create_database(self, database_name):
        mysql = self.conn.cursor()
        mysql.execute("CREATE DATABASE " + database_name)

    def create_table_candlestick(self, table_name):
        mysql = self.conn.cursor()
        mysql.execute(f"CREATE TABLE {table_name} " \
                    "(id INT AUTO_INCREMENT PRIMARY KEY, ticker VARCHAR(8), market VARCHAR(8), dt DATETIME, open DOUBLE, " \
                    "close DOUBLE, " \
                    "high DOUBLE, low DOUBLE, volume INT, dividends DOUBLE, stock_splits DOUBLE)")

