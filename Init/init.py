import Database.build_database as build
import Database.build_tables as tables
from Data.config_read import config as get_values
import Data.build_stock_list as fill
import Interface.Interface as interface
import Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
##########################################
# Runs at start up
# Gets the status of the databases, brings up a menu and gives the user choices as to what to do
# Contains functions to load all data (up to 2 years back)
###########################################

class init:
    def __init__(self):
        pass

    def startup(self):
        build.build_database().build_database()
        self.get_database_status()
        self.get_list_status()
        # tables.build_tables().build_tables()
        # self.update_all_tables()
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