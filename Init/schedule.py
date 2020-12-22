import schedule
import keyboard
from halo import Halo
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Technical_Data.Retrieve_Data.retrieve_sentiment as rs
import Data.config_read as config

class schedule_assist:
    def __init__(self):
        con = config.config()
        self.daily_schedule_run_time = con.daily_schedule_run_time
        self.weekly_schedule_run_time = con.weekly_schedule_run_time

    def daily_schedule_function_group(self):
        rt.retrieve_technical_data().run_data_load()
        rs.retrieve_sentiment_data().run_data_load()
        # email.send_email().daily_update_email()

    def weekly_schedule_function_group(self):
        pass
        # build_list.stock_list.list_to_db()
        # build_table.build_tables.build_tables()
        # utility.utility().clean_stock_data()
        # email.send_email().weekly_update_email()

    def run_schedule(self):
        run = True
        print("Schedule is now running... Press 'q' at any time to quit schedule")
        # spinner = Halo(text='Running...', spinner='dots', color='grey', interval=300)
        # spinner.start()
        schedule.every().day.at(self.daily_schedule_run_time).do(self.daily_schedule_function_group)
        schedule.every().saturday.at(self.weekly_schedule_run_time).do(self.weekly_schedule_function_group)

        while run:
            schedule.run_pending()
            if keyboard.is_pressed('q'):
                run = False
                # spinner.stop()

