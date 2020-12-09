import Database.build_database as database_build
import Data.stock_list as fill
import Interface.Interface as interface
import Data.Technical_Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Database.build_tables as tables_build
##########################################
# Runs at start up
# Gets the status of the databases, brings up a menu and gives the user choices as to what to do
# Contains functions to load all data (up to 2 years back)
###########################################

class init:
    def __init__(self):
        pass

    def startup(self):
        database_build.build_database().build_database()
        self.get_database_status()
        self.get_list_status()
        tables_build.build_tables().build_tables()
        answer = input("""
        Would you like to...
        1. Update Data Tables
        2. Skip 
        """)
        if answer == "1" or answer.lower() == "update data tables":
            self.update_all_tables()
        interface.view().start_up()


    def get_list_status(self):
        #Check last update on list
        #Check last update on stock table
        pass


    def get_database_status(self):
        pass


    def fill_stock_list_table(self):
        fill.stock_list().list_to_db()


    def update_all_tables(self):
        print("--------------------------------------------")
        print("Beginning update of all stock data tables...")
        yfinance.yfinance().update_data()
        print("Beginning update of all sentiment data tables...")
        sentiment.sentiment().gather_headlines()