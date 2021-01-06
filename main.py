from Init.init import startup
from flask_cors import CORS
from flask import Flask
from Init.schedule import get_data
from Init.schedule import run_schedule as sched_run
from Init.schedule import stop_schedule as sched_stop
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment_bulk as rs
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
from WebApp.Controller.flask_init import create_app
import Data.Prediction_Data.Retrieve_Data.linear_regression as lr
import datetime
import Data.stock_list as st_list
import time

app = create_app()
CORS(app)

if __name__ == '__main__':
    get_data()
    # startup()
    # app.run(debug=True, use_reloader=True)
    # print(lr.linear_regression().retrieve_data('AAPL'))





