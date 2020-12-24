from Database.database import database

from Data.config_read import config as get_values


class crud:
    def __init__(self):
        config = get_values()
        self.conn = database().conn_finance
        self.stock_table_list_name = "STOCK_LIST_TBL"

    def get_list_of_stocks(self):
        sql_statement = f"SELECT ticker FROM STOCK_LIST_TBL;"

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_statement)
            stock_list = cursor.fetchall()
            return stock_list
        except:
            database.insert_error_log("ERROR FETCHING STOCK_LIST_TBL: " + self.conn.get_warnings)
            print("ERROR FETCHING STOCK_LIST_TBL")

