import functools
import Database.QueryHandler as qh
from flask import Blueprint
from flask import flash
from flask import g
from flask import redirect
from flask import render_template
from flask import request
from flask import session
from flask import url_for
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash
import random
import json
from Utility.config import global_dict

bp = Blueprint("auth", __name__, url_prefix="/auth")

@bp.route('/login', methods=('GET', 'POST'))
def login():
    if request.method == 'POST':
        data = request.json
        result = qh.query_handler().login(data['username'], data['password'])
        token = random.randint(100, 9999)
        global_dict['token'] = token
        if result:
            return {
                'token' : token
            }
        else:
            return 'Invalid username or password'
    else:
        return "Invalid request"


