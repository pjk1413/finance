from consolemenu import *
from consolemenu.items import *
import os
import Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Init.schedule as schedule
import Database.build_tables as tables
from Data.config_read import config as get_values
import Database.email_updates as email
from Database.database import database
import Database.clean as clean

class view:
    def __init__(self):
        config = get_values()
        self.daily_schedule_run_time = config.daily_schedule_run_time
        self.weekly_schedule_run_time = config.weekly_schedule_run_time


    def main_menu(self):
        main_menu = ConsoleMenu("Welcome to Finance.Quant")

        main_menu_item_list = [
            FunctionItem("Update all Data", self.gather_data),
            FunctionItem("Build All Tables", tables.build_tables().build_tables),
            FunctionItem("Clean All Data", self.clean_data),
            FunctionItem("Hard Reset", self.confirmation_menu, [self.reset_program]),
            FunctionItem("Edit Configuration File", self.edit_config, menu=main_menu),
            FunctionItem("View Schedule", self.view_schedule),
            FunctionItem("Run Schedule", self.run_schedule)
        ]

        for item in main_menu_item_list:
            main_menu.append_item(item)

        main_menu.show()


    def clean_data(self):
        pass


    def confirmation_menu(self, function):
        confirmation = ConsoleMenu(title="Are you sure?", show_exit_option=False)

        confirmation_menu_item_list = [
            FunctionItem("Yes", function),
            FunctionItem("No", self.main_menu)
        ]

        for item in confirmation_menu_item_list:
            confirmation.append_item(item)

        confirmation.show()


    def gather_data(self):
        print("Beginning update of all stock data tables...")
        yfinance.yfinance().update_data()
        database().insert_status_log("UPDATED ALL STOCK DATA TABLES")
        print("Beginning update of all sentiment data tables...")
        sentiment.sentiment().gather_headlines()
        database().insert_status_log("UPDATED ALL SENTIMENT DATA TABLES")


    def run_schedule(self):
        schedule.schedule_assist().run_schedule()


    def view_schedule(self):
        print(f"""
        Daily Time: {self.daily_schedule_run_time}
        Weekly Time: {self.weekly_schedule_run_time}
        """)
        input("Press any key to continue...")


    def reset_program(self):
        # Will erase everything
        print("This will erase everything")
        input("Press any key to continue...")


    def edit_config(self):
        os.system("start config.txt")


    def start_up(self):
        self.main_menu()