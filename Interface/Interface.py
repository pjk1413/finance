from consolemenu import *
from consolemenu.items import *
import os
import Data.yahoo_finance as yfinance
import Data.sentiment as sentiment
import Init.schedule as schedule
import Database.build_tables as tables

class view:
    def __int__(self):
        pass


    def main_menu(self):
        main_menu = ConsoleMenu("Welcome to Finance.Quant")

        main_menu_item_list = [
            FunctionItem("Update all Data", self.gather_data),
            FunctionItem("Build All Tables", tables.build_tables().build_tables),
            FunctionItem("Hard Reset", self.confirmation_menu, [self.reset_program]),
            FunctionItem("Edit Configuration File", self.edit_config, menu=main_menu),
            FunctionItem("View Schedule", self.view_schedule),
            FunctionItem("Run Schedule", self.run_schedule)
        ]

        for item in main_menu_item_list:
            main_menu.append_item(item)

        main_menu.show()


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
        print("Beginning update of all sentiment data tables...")
        sentiment.sentiment().gather_headlines()


    def run_schedule(self):
        schedule.schedule_assist().run_schedule()


    def view_schedule(self):
        pass


    def reset_program(self):
        # Will erase everything
        print("reset")


    def edit_config(self):
        os.system("start config.txt")


    def start_up(self):
        self.main_menu()