import schedule
import keyboard
from halo import Halo

import Data.config_read as config
import Data.Technical_Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Data.utility as utility
import Data.build_stock_list as build_list
import Database.build_tables as build_table
import Database.email_updates as email

class schedule_assist:
    def __init__(self):
        con = config.config()
        self.daily_schedule_run_time = con.daily_schedule_run_time
        self.weekly_schedule_run_time = con.weekly_schedule_run_time


    def daily_schedule_function_group(self):
        yfinance.yfinance().update_data()
        sentiment.sentiment().gather_headlines()
        email.send_email().daily_update_email()


    def weekly_schedule_function_group(self):
        build_list.stock_list.list_to_db()
        build_table.build_tables.build_tables()
        utility.utility().clean_stock_data()
        email.send_email().weekly_update_email()


    def run_schedule(self):
        run = True
        print("Schedule is now running... Press 'q' at any time to quit schedule")
        spinner = Halo(text='Running...', spinner='dots', color='grey', interval=300)
        spinner.start()
        schedule.every().day.at(self.daily_schedule_run_time).do(self.daily_schedule_function_group)
        schedule.every().saturday.at(self.weekly_schedule_run_time).do(self.weekly_schedule_function_group)

        while run:
            schedule.run_pending()
            if keyboard.is_pressed('q'):
                run = False
                spinner.stop()

