
import Data.config_read as con
from threading import Thread
import pymysql
from dbutils.pooled_db import PooledDB
import mysql.connector.errors as err
from time import sleep

class mySqlObj:
    def __init__(self, connection, sql_statement):
        self.conn = connection
        self.cursor = connection.cursor()
        self.statement = sql_statement


class Multi_Threading:
    def __init__(self, list_of_statements, database):
        config = con.config()
        self.stock_db = config.stock_db_name
        self.sentiment_db = config.sentiment_db_name
        self.fundamental_db = config.fundamental_db_name
        self.list = list_of_statements
        self.thread_list = []
        self.conn_pool = PooledDB(creator=pymysql,
                               host=f'{config.db_host}',
                               user=f'{config.db_user}',
                               password=f'{config.db_pass}',
                               database=f'{self.getPool(database)}',
                               autocommit=True,
                               blocking=True,
                               maxconnections=5)

    def getPool(self, database):
        if database == "technical":
            return self.stock_db
        elif database == "sentiment":
            return self.sentiment_db
        elif database == "fundamental":
            return self.fundamental_db

    def run_insert(self):
        for i, statement in enumerate(self.list):
            try:
                connection = self.conn_pool.connection()
            except err.InterfaceError as error:
                print(f"INTERFACE ERROR: {error}")
                sleep(2)
            t = Thread(target=execute_insert_statement, args=(i, connection ,statement))
            self.thread_list.append(t)
            t.start()

        for t in self.thread_list:
            t.join()

    def run(self):
        for i, statement in enumerate(self.list):
            try:
                connection = self.conn_pool.connection()
            except err.InterfaceError as error:
                print(f"INTERFACE ERROR: {error}")
                sleep(2)
            t = Thread(target=execute_statement, args=(i, connection, statement))
            self.thread_list.append(t)
            t.start()

        for t in self.thread_list:
            t.join()

def execute_statement(num, conn, statement):
    try:
        cursor = conn.cursor()
        cursor.execute(statement)
        cursor.close()
        conn.close()
    except err.InterfaceError as error:
        print(f"INTERFACE ERROR: {error}")
        sleep(2)
    except err:
        print(err)

def execute_insert_statement(num, conn, statement):
    try:
        cursor = conn.cursor()
        cursor.execute(statement)
        conn.commit()
        cursor.close()
        conn.close()
    except err.InterfaceError as error:
        print(f"INTERFACE ERROR: {error}")
        sleep(2)
    except err:
        print(err)