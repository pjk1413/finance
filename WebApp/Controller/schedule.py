import functools
import Database.QueryHandler as qh
import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
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

@bp.route('/run')
def run_schedule():
    subprocess.call('start python run_subprocess.py run_schedule', shell=True)
    return 'Schedule started'

@bp.route('/stop')
def stop_schedule():
    global_dict['run'] = False
    return 'Schedule stopped'