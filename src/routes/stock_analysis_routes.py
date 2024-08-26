from flask import Blueprint
from src.controllers.stock_analysis_controller import *

stock_analysis_bp = Blueprint("stockanalysis", __name__, url_prefix="/stockanalysis")

@stock_analysis_bp.route("/overall/stockposition", methods=["GET"])
def get_overall_stock_position():
    return get_overall_stock_position_controller()

@stock_analysis_bp.route("/overall/newandstockposition", methods=["GET"])
def get_overall_new_and_stock_position():
    return get_overall_new_and_stock_position_controller()

@stock_analysis_bp.route("/overall/stockageing", methods=["GET"])
def get_overall_stock_ageing():
    return get_overall_stock_ageing_controller()

@stock_analysis_bp.route("/overall/scbsqb", methods=["GET"])
def get_overall_sales_category_by_stock_qty_bucket():
    return get_overall_sales_category_by_stock_qty_bucket_controller()

@stock_analysis_bp.route("/overall/dsi", methods=["GET"])
def get_overall_dsi_by_saleable_qty_bucket():
    return get_overall_dsi_by_saleable_qty_bucket_controller()

@stock_analysis_bp.route("/overall/itemlevel", methods=["GET"])
def get_overall_item_level_details():
    return get_overall_item_level_details_controller()

@stock_analysis_bp.route("/overall/search", methods=["GET"])
def overall_stock_analysis_search():
    return overall_stock_analysis_search_controller()

@stock_analysis_bp.route("/shoplevel/stockposition", methods=["GET"])
def get_shop_level_stock_position():
    return get_shop_level_stock_position_controller()

@stock_analysis_bp.route("/shoplevel/newandstockposition", methods=["GET"])
def get_shop_level_new_and_stock_position():
    return get_shop_level_new_and_stock_position_controller()

@stock_analysis_bp.route("/shoplevel/stockageing", methods=["GET"])
def get_shop_level_stock_ageing():
    return get_shop_level_stock_ageing_controller()

@stock_analysis_bp.route("/shoplevel/scbsqb", methods=["GET"])
def get_shop_level_sales_category_by_stock_qty_bucket():
    return get_shop_level_sales_category_by_stock_qty_bucket_controller()

@stock_analysis_bp.route("/shoplevel/dsi", methods=["GET"])
def get_shop_level_dsi_by_saleable_qty_bucket():
    return get_shop_level_dsi_by_saleable_qty_bucket_controller()

@stock_analysis_bp.route("/shoplevel/storelevel", methods=["GET"])
def get_shop_level_store_level_details():
    return get_shop_level_store_level_details_controller()

@stock_analysis_bp.route("/shoplevel/search", methods=["GET"])
def shop_level_stock_analysis_search():
    return shop_level_stock_analysis_search_controller()