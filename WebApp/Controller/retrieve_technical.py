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
from Utility.config import global_dict

bp = Blueprint("technical", __name__, url_prefix="/stock")

@bp.route('/find/<ticker>')
def find_stock(ticker):
    return qh.query_handler().run_select_query('stock', 'stock_data', where_args=[('ticker', f'{ticker}')])

@bp.route('/load_data/<token>')
def run_data_load(token):
    if int(token) == global_dict['token']:
        subprocess.call('start python run_subprocess.py retrieve_technical_data', shell=True)
        return "Data Load Started"
    else:
        return "Invalid token, try logging in again"

