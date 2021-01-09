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
from Utility.config import global_dict
import run_subprocess

bp = Blueprint("schedule", __name__, url_prefix="/schedule")
schedule_process = None

@bp.route('/status', methods=['GET'])
def status():
    if schedule_process == None:
        return 'False'
    else:
        return 'True'

@bp.route('/get', methods=['GET'])
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
    run_subprocess.add_process('schedule', schedule.run_schedule)
    schedule_process = True
    return {
        'schedule': True
    }

@bp.route('/stop')
def stop_schedule():
    global schedule_process
    run_subprocess.terminate_process('schedule')
    schedule_process = None
    return {
        'schedule': False
    }