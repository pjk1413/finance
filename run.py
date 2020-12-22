import Data.Init_Gather.gather_stock_data as gsd
import sys

print("Starting to gather stock ticker symbols and extra data...")
run = True
# Host, User, Password, Stock_DB, alphavantage_api_key
# args = sys.argv[1:]
while run:
    program = gsd.gather_stock_data()
    print("Updating stock list...")
    program.update_stock_list()
    print("Updating stock data...")
    program.gather_extra_data()
