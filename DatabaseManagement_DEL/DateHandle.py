import datetime
import pandas as pd
import numpy as np
import re

def convert_datetime_float(date):
    return pd.to_datetime(date).to_datetime()

def convert_datetime_mysql(date):
    return pd.to_datetime()

def convert_to_datetime(date):
    str_time = np.datetime_as_string(date)

    date_time_split = str_time.split("T")

    year = int(date_time_split[0].split("-")[0])
    month = int(date_time_split[0].split("-")[1])
    day = int(date_time_split[0].split("-")[2])

    hour = int(date_time_split[1].split(":")[0])
    minute = int(date_time_split[1].split(":")[1])
    second = int(date_time_split[1].split(":")[2].split(".")[0])

    timestamp = datetime.datetime(year, month, day, hour, minute, second)
    return timestamp

def validate_date_range(self):
    """
    Need to validate the date range in a different manner for the robinhood_api, not used
    :return:
    """
    if self.date_range == 'max':
        return False

    for date in self.date_range:
        if not re.match("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}", date):
            print("Alphavantage Date Format Error - Date must be astring in the YYYY-MM-DD format or 'max'")
            self.sql.log_error(f"Alphavantage date range given is not valid for: '{self.date_range}'")
            return False
    return True