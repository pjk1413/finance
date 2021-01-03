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

bp = Blueprint("sentiment", __name__, url_prefix="/sentiment")

@bp.route('/find/<ticker>')
def find_stock_sentiment(ticker):
    return 'sentiment data'

@bp.route('/load_data/<token>')
def run_data_load(token):
    if int(token) == global_dict['token']:
        subprocess.call('start python run_subprocess.py retrieve_sentiment_data', shell=True)
        return "Data Load Started"
    else:
        return "Invalid token, try logging in again"
