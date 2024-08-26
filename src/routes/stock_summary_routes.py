from flask import Blueprint
from src.controllers.stock_summary_controller import *
from src.utils.jwt_token_utils import token_required

stock_summary_bp = Blueprint("stocksummary", __name__, url_prefix="/stocksummary")

@stock_summary_bp.route("/headoffice/", methods=["GET"])
def get_headoffice_stock_summary_list():
    return get_headoffice_stock_summary_list_controller()

# @stock_summary_bp.route("/store/<string:store_code>", methods=["GET"])
@stock_summary_bp.route("/store/<string:store_code>", methods=["GET"])
def get_store_stock_summary_list(store_code):
    return get_store_stock_summary_list_controller(store_code)
    