import mysql.connector.errors as error
import Database.database as db
import datetime

def find_most_recent_date(stock):
    """
    Finds the most recent entry in the database
    :param ticker symbol
    :return: datetime object representing most recent date entered into database
    """
    sql_statement = f"SELECT dt FROM STOCK_DATA WHERE ticker='{stock}' ORDER BY dt DESC LIMIT 1;"
    try:
        connection = db.database().conn_stock
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        recent_date = cursor.fetchone()
    except error:
        print(error)
    return recent_date