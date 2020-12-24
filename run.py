import Data.Init_Gather.gather_stock_data as gsd
import time

print("Starting to gather stock ticker symbols and extra data...")
run = True
time.sleep(500)
while run:
    program = gsd.gather_stock_data()
    print("Updating stock list...")
    program.update_stock_list()
    print("Updating stock data...")
    program.gather_extra_data()
