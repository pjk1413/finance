import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import Database.Service.database as database
from mysql.connector import errors as err

class linear_regression:
    def __init__(self):
        self.conn_stock = database.database().conn_stock

    def get_dataframe(self, ticker):
        sql_statement = f"SELECT * FROM stock_technical_data.stock_data WHERE ticker='{ticker}';"
        try:
            cursor = self.conn_stock.cursor(dictionary=True)
            cursor.execute(sql_statement)
            results = cursor.fetchall()
            # print(results)
        except err:
            print(err)
            print("ERROR CREATING USER TABLE")
        dataframe = pd.DataFrame.from_dict(results)
        # dataframe = pd.DataFrame(results, columns=['id', 'ticker', 'date', 'open', 'close', 'high', 'low', 'adj_close', 'volume', 'split', 'dividend'])
        return dataframe

    def get_linear_regression(self, ticker):
        data = self.get_dataframe(ticker)
        X = data.iloc[:,2:].values
        y = data.iloc[:, -1].values

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

        regressor = LinearRegression()
        regressor.fit(X_train, y_train)
        LinearRegression(copy_X=True, fit_intercept=True, n_jobs=None, normalize=False)

        y_pred = regressor.predict(X_test)
        print(y_pred)
