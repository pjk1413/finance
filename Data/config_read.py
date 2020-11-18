

class config:
    def __init__(self):
        self.nasdaq_listed_url = self.get_value("nasdaq_listed_url")
        self.nyse_listed_url = self.get_value("nyse_listed_url")
        self.amex_listed_url = self.get_value("amex_listed_url")
        self.stock_db_name = self.get_value("stock_db_name")
        self.db_user_root = self.get_value("db_user_root")
        self.db_pass_root = self.get_value("db_pass_root")
        self.db_user = self.get_value("db_user")
        self.db_pass = self.get_value("db_pass")
        self.db_host = self.get_value("db_host")
        self.file_location = self.get_value("file_location")
        self.replace_file = self.get_value("replace_file")
        self.refresh_symbols = self.get_value("refresh_symbols")
        self.first_start = self.get_value("first_start")
        self.finwiz_url = self.get_value("finwiz_url")
        self.sentiment_db_name = self.get_value("sentiment_db_name")
        self.daily_schedule_run_time = self.get_value("daily_schedule_run_time")
        self.weekly_schedule_run_time = self.get_value("weekly_schedule_run_time")
        self.email_username = self.get_value("email_username")
        self.email_password = self.get_value("email_password")
        self.utility_db_name = self.get_value("utility_db_name")
        self.predict_db_name = self.get_value("predict_db_name")

    def get_value(self, str):
        file = open("config.txt")

        for line in file:
            if line.find("=") == -1:
                continue
            if line[0-2] == "##":
                continue
            name = line.split("=")[0]
            value = line.split("=")[1].strip()

            if name == str:
                return value