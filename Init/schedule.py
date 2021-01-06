import schedule
import keyboard
import Database.Service.database as db
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment as rs
import Data.Init_Gather.gather_stock_data as gsd
import Database.Build.build_tables as build_tables
import config_read as config
import json
from Utility.global_ import global_dict
import os


con = config.config()
# TODO alter where schedule times come from
daily_schedule_run_time = con.daily_schedule_run_time
weekly_schedule_run_time = con.weekly_schedule_run_time

def get_data():
    sql_statement = f"SELECT * from schedules;"
    try:
        conn = db.database().conn_utility
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql_statement)
        results = cursor.fetchall()
        cursor.close()
        return {
            'schedules': json.dumps(results)
        }
    except:
        print("ERROR")

def update_start_time(description, time, frequency):
    sql_statement = f"REPLACE INTO schedules (description, time, frequency) VALUES('{description}', {time}, '{frequency}');"
    try:
        conn = db.database().conn_utility
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        cursor.close()
    except:
        print("ERROR")

def daily_schedule():
    rt.retrieve_technical_bulk().run_data_load()
    rs.retrieve_sentiment_data().run_data_load()
    # email.send_email().daily_update_email()

def weekly_schedule():
    gsd.gather_stock_data().update_stock_list()
    build_tables.build_tables().build_tables()
    # utility.utility().clean_stock_data()
    # email.send_email().weekly_update_email()

def run_schedule():
    run = True
    schedule.every().day.at(daily_schedule_run_time).do(daily_schedule)
    schedule.every().saturday.at(weekly_schedule_run_time).do(weekly_schedule)
    while run:
        schedule.run_pending()

def stop_schedule():
    print("Stop")

