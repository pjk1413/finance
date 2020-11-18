import yfinance as yf
from Init.init import init as init
from Database.crud_func import crud
import Database.build_database as db
import Database.build_tables as build
from Database.database import database

import Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Interface.Interface as interface


if __name__ == '__main__':
    init().startup()








