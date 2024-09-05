from flask import Blueprint
from src.controllers.auth_controller import *

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

@auth_bp.route('/register', methods=['POST'])
def register():
    return register_user_controller()

@auth_bp.route('/login', methods=['POST'])
def login():
    return login_user_controller()

@auth_bp.route('/user_import', methods=['POST'])
def user_import():
    return import_users_from_csv()

# @auth_bp.route('/logout', methods=['POST'])
# def user_logout():
#     return logout_user_controller()