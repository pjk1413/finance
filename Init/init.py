import Data.stock_list as fill
import Interface.Interface as interface
import os

from Database.Build import build_database, build_tables

def startup():
    build_database.build_database().build_database()
    build_tables.build_tables().build_tables()
    interface.view().start_up()

def get_list_status():
    #Check last update on list
    #Check last update on stock table
    pass

def get_database_status():
    pass

def fill_stock_list_table():
    fill.stock_list().list_to_db()