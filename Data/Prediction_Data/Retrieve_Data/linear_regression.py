import numpy as np
from sklearn.linear_model import LinearRegression
import mysql.connector.errors as err
import datetime

from Database.Service import database

# https://realpython.com/linear-regression-in-python/

class linear_regression:
    def __init__(self):
        self.conn_stock = database.database().conn_stock
        self.start_date = datetime.datetime.strptime('2018-01-01', "%Y-%m-%d")
        pass

    def retrieve_data(self, ticker, candle='adj_close'):
        x, y = [], []
        sql_statement = f"SELECT dt, {candle} FROM stock_technical_data.stock_data WHERE ticker='{ticker}';"
        try:
            cursor = self.conn_stock.cursor()
            cursor.execute(sql_statement)
            results = cursor.fetchall()
        except err:
            print("ERROR SELECTING DATA")

        for entry in results:
            delta = entry[0] - self.start_date
            x.append(delta.days)
            y.append(entry[1])

        x = np.array(x).reshape((-1, 1))
        y = np.array(y)

        model = LinearRegression()
        model.fit(x,y)
        r_sq = model.score(x,y)
        intercept = model.intercept_
        slope = model.coef_
        y_pred_14 = model.predict([x[-1]+14])
        y_pred_30 = model.predict([x[-1]+30])
        return {
            'Current_Price': y[-1],
            'R-Squared':r_sq,
            'Intercept': intercept,
            'Slope': slope,
            '14_Days': y_pred_14,
            '30_Days': y_pred_30
        }