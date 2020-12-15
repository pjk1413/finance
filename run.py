import Data.Init_Gather.gather_stock_data as gsd
import sys

print("Starting to gather stock ticker symbols and extra data...")
run = True
# Host, User, Password, Stock_DB, alphavantage_api_key
# args = sys.argv[1:]
while run:
    program = gsd.gather_stock_data()
    program.update_stock_list()
    program.gather_extra_data()
