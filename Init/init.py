import Data.stock_list as fill
import Interface.Interface as interface
from Logger.logger import log_status
from Logger.logger import log_error
import subprocess

from Database.Build import build_database, build_tables

def startup():
    log_status("---------- Starting Up ----------")
    log_error("---------- Starting Up ----------")
    build_database.build_database().build_database()
    build_tables.build_tables().build_tables()
    # start_flask_server()

    # interface.view().start_up()

def get_list_status():
    #Check last update on list
    #Check last update on stock table
    pass

def get_database_status():
    pass

def fill_stock_list_table():
    fill.stock_list().list_to_db()
    pass

# def start_flask_server():
#     try:
#         subprocess.call('start python server_setup.py', shell=True)
#         log_status("Flask server started successfully")
#     except:
#         log_error("Could not start Flask server")
