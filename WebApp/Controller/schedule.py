import functools
import Database.QueryHandler as qh
import Init.schedule as schedule
from flask import Blueprint
import subprocess
from flask import flash
from flask import g
from flask import redirect
from flask import render_template
from flask import request
from flask import session
from flask import url_for
from Utility.global_ import global_dict

bp = Blueprint("schedule", __name__, url_prefix="/schedule")
schedule_process = None

@bp.route('/get')
def get_data():
    return schedule.get_data()

@bp.route('/change-start-time/<description>/<time>/<frequency>')
def change_start_time(description, time, frequency):
    # time format 00:00
    schedule.update_start_time(description, time, frequency)
    return {
        'description': description,
        'time': time,
        'frequency': frequency
    }

@bp.route('/start')
def run_schedule():
    global schedule_process
    schedule_process = subprocess.Popen('start python run_subprocess.py run_schedule', shell=True, stdout=subprocess.PIPE)
    return {
        'schedule': True
    }

@bp.route('/stop')
def stop_schedule():
    global schedule_process
    schedule_process.terminate()
    return {
        'schedule': False
    }