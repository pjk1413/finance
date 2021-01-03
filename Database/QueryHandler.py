import json
import mysql.connector.errors as err
import Database.Service.database as db
import datetime

class create_dict(dict):
    # __init__ function
    def __init__(self):
        self = dict()

        # Function to add key:value
    def add(self, key, value):
        self[key] = value

class query_handler:
    def __init__(self):
        self.conn_stock = db.database().conn_stock
        self.conn_sentiment = db.database().conn_sentiment
        self.conn_utility = db.database().conn_utility

    def get_database(self, database):
        if database == 'stock':
            return self.conn_stock
        elif database == 'sentiment':
            return self.conn_sentiment
        elif database == 'utility':
            return self.conn_utility

    def login(self, username, password):
        sql_statement = "SELECT * from users;"

        try:
            cursor = self.conn_utility.cursor()
            cursor.execute(sql_statement)
            results = cursor.fetchall()
        except:
            print("ERROR")

        for entry in results:
            if entry[1] == username and entry[2] == password:
                return True


    def build_select_query(self, table, where_args):
        sql_statement = f"SELECT * FROM {table} "

        if len(where_args) > 0:
            sql_statement += "WHERE "
            for entry in where_args:
                sql_statement += f"{entry[0]} = '{entry[1]}' AND "
            sql_statement = sql_statement[:-4]

        sql_statement += ";"
        return sql_statement

    def run_select_query(self, database, table, where_args=[]):
        db = self.get_database(database)
        sql_statement = self.build_select_query(table, where_args)

        try:
            cursor = db.cursor(dictionary=True)
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            cursor.close()
        except err:
            print(err)
            print("ERROR")

        json_results = json.dumps(results, indent=4, default=str)
        return json_results