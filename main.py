import Database.build_database as build_database
import Database.build_tables as build_tables
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Technical_Data.Retrieve_Data.retrieve_sentiment as rs
import Init.schedule as schedule
import Interface.Interface as Interface
import Data.stock_list as stock_list
import Data.Init_Gather.gather_stock_data as gsd

def startup(run_historical):
    build_database.build_database().build_database()
    build_tables.build_tables().build_tables()
    if run_historical:
        rt.retrieve_technical_data().run_data_load(range='historical')
        rs.retrieve_sentiment_data().run_data_load(range='historical')
    run()

def run():
    schedule.schedule_assist().run_schedule()

if __name__ == '__main__':
    startup(True)







