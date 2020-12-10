import Data.Technical_Data.get_data as data
import Data.Technical_Data.Model.stock_model as stock
import Data.Technical_Data.Model.stock_model as stock
import Database.build_database as build_database
import Database.build_tables as build_tables
import Init.init as init
from Init import init
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Technical_Data.Retrieve_Data.retrieve_indicator as ri
import Data.Technical_Data.Retrieve_Data.retrieve_sentiment as rs
import Init.schedule as schedule

import Database.Service.sentiment_service as ss


def startup(run_historical):
    build_database.build_database().build_database()
    build_tables.build_tables().build_tables()
    if run_historical:
        rt.retrieve_technical_data().run_data_load(range='historical')
        rs.retrieve_sentiment_data().run_data_load(range='historical')


def run():
    schedule.schedule_assist().run_schedule()


if __name__ == '__main__':
    startup(True)
    run()






