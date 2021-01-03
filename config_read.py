

class config:
    def __init__(self):
        # Database Configurations
        self.stock_db_name = self.get_value("stock_db_name")
        self.sentiment_db_name = self.get_value("sentiment_db_name")
        self.utility_db_name = self.get_value("utility_db_name")
        self.predict_db_name = self.get_value("predict_db_name")
        self.fundamental_db_name = self.get_value("fundamental_db_name")
        self.db_port = self.get_value("db_port")
        self.character_set = self.get_value("character_set")
        self.db_user_root = self.get_value("db_user_root")
        self.db_pass_root = self.get_value("db_pass_root")
        self.db_user = self.get_value("db_user")
        self.db_pass = self.get_value("db_pass")
        self.db_host = self.get_value("db_host")

        # File handling
        self.file_location = self.get_value("file_location")
        self.replace_file = self.get_value("replace_file")
        self.daily_schedule_run_time = self.get_value("daily_schedule_run_time")
        self.weekly_schedule_run_time = self.get_value("weekly_schedule_run_time")
        self.log_file = self.get_value("log_file")

        # Email Configurations
        self.email_username = self.get_value("email_username")
        self.email_password = self.get_value("email_password")
        self.email_receive = self.get_value("email_receive")

        # API Keys
        self.alpha_vantage_api_key = self.get_value("alpha_vantage_api_key")
        self.tiingo_api_key = self.get_value("tiingo_api_key")

        # IO Config
        self.thread_number = int(self.get_value("number_of_threads"))
        self.process_number = int(self.get_value("number_of_processes"))

        # Technical Configuration
        self.sma_periods = self.get_values_list("sma_periods", return_type="num")
        self.ema_periods = self.get_values_list("ema_periods", return_type="num")
        self.ema_smoothing = int(self.get_value("ema_smoothing"))

    def get_schedule_run_times(self):
        sql_statement = "SELECT "

    def get_values_list(self, str, return_type= "str"):
        file = open("config.txt")

        for line in file:
            if line.find("=") == -1:
                continue
            if line[0-2] == "##":
                continue
            name = line.split("=")[0]
            value = line.split("=")[1].strip()

            if name == str:
                if return_type == "num":
                    value = value.split(",")
                    return [int(i) for i in value]
                if return_type == "str":
                    return value.split(",")

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