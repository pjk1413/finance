import mysql.connector as connect
from Data.config_read import config as get_values
import datetime
import mysql.connector.errors as error
from time import sleep
from Database.Service.email_service import warning_email

class database:
    def __init__(self):
        config = get_values()
        self.root_user = config.db_user_root
        self.root_pass = config.db_pass_root
        self.user = config.db_user
        self.password = config.db_pass
        self.host = config.db_host
        self.port = config.db_port
        self.stock_db = config.stock_db_name
        self.sentiment_db = config.sentiment_db_name
        self.utility_db = config.utility_db_name
        self.fundamental_db = config.fundamental_db_name
        self.predict_db = config.predict_db_name
        self.log_file = config.log_file
        try:
            self.conn_stock = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.stock_db}"
            )
        except error.InterfaceError:
            print("Error opening connection STOCK")
            sleep(2)
        except error:
            print(f"Error opening connection {error}")
        try:
            self.conn_sentiment = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.sentiment_db}",
                port=f"{self.port}"
            )
        except error.InterfaceError:
            print("Error opening connection SENTIMENT")
            sleep(2)
        except error:
            print(f"Error opening connection {error}")
        try:
            self.conn_utility = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.utility_db}",
                port=f"{self.port}"
            )
        except error.InterfaceError:
            print("Error opening connection UTILITY")
            sleep(2)
        except error:
            print(f"Error opening connection {error}")
        try:
            self.conn_fundamental = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.fundamental_db}",
                port=f"{self.port}"
            )
        except error.InterfaceError:
            print("Error opening connection FUNDAMENTAL")
            sleep(2)
        except error:
            print(f"Error opening connection {error}")
            self.conn_predict = connect.connect(
                host=f"{self.host}",
                user=f"{self.user}",
                password=f"{self.password}",
                database=f"{self.predict_db}",
                port=f"{self.port}"
            )
        except error.InterfaceError:
            print("Error opening connection PREDICT")
            sleep(2)
        except error:
            print(f"Error opening connection {error}")

def insert_log_statement(statement):
    config = get_values()
    f = open(config.log_file, "a")
    f.write(statement + "\n")
    f.close()
    print(statement)


def insert_status_log(statement):
    config = get_values()
    insert_log_statement(f"STATUS: {statement}")
    sql_statement = "INSERT INTO STATUS_TBL (dt, statement) " \
                    "VALUES (%s, %s)"
    values = (datetime.datetime.now(), statement)
    try:
        conn = connect.connect(
                host=f"{config.db_host}",
                user=f"{config.db_user}",
                password=f"{config.db_pass}",
                database=f"{config.utility_db_name}")
        cursor = conn.cursor()
        cursor.execute(sql_statement, values)
        conn.commit()
        return True
    except error:
        print(error)
        return False


def insert_error_log(statement):
    config = get_values()
    insert_log_statement(f"ERROR: {statement}")
    sql_statement = "INSERT INTO error_log (dt, description) " \
                    "VALUES (%s, %s)"
    values = (datetime.datetime.now(), statement)

    if 'ERROR' in statement:
        warning_email(statement)
    try:
        conn = connect.connect(
            host=f"{config.db_host}",
            user=f"{config.db_user}",
            password=f"{config.db_pass}",
            database=f"{config.utility_db_name}")
        cursor = conn.cursor()
        cursor.execute(sql_statement, values)
        conn.commit()
        return True
    except error:
        print(error)
        return False









