from Init.init import startup
from flask import Flask
from Init.schedule import run_schedule as sched_run
from Init.schedule import stop_schedule as sched_stop
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment as rs
from WebApp.Controller.flask_init import create_app

app = create_app()

if __name__ == '__main__':
    startup()
    app.run(debug=True, use_reloader=True)






