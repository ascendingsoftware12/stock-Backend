from flask import Blueprint
from src.controllers.sales_all_in_one_live_controller import *

sales_all_in_one_live_bp = Blueprint("sales_all_in_one_live", __name__, url_prefix="/sales_all_in_one_live")

# -----------------------------------------------------
# Get all
# -----------------------------------------------------

@sales_all_in_one_live_bp.route("/", methods=["GET"])
def get_sales_all_in_one_live():
    return get_sales_all_in_one_live_controller()

# -----------------------------------------------------
# ytd
# -----------------------------------------------------

@sales_all_in_one_live_bp.route("/ytd", methods=["GET"])
def get_sales_all_in_one_live_ytd():
    return get_sales_all_in_one_live_ytd_controller()

# -----------------------------------------------------
# month 
# -----------------------------------------------------

@sales_all_in_one_live_bp.route("/month_cr", methods=["GET"])
def get_sales_all_in_one_live_month_cr():
    return get_sales_all_in_one_live_month_cr_controller()


@sales_all_in_one_live_bp.route("/month_cr_without_gst", methods=["GET"])
def get_sales_all_in_one_live_month_cr_without_gst():
    return get_sales_all_in_one_live_month_cr_without_gst_controller()


@sales_all_in_one_live_bp.route("/month_lk", methods=["GET"])
def get_sales_all_in_one_live_month_lk():
    return get_sales_all_in_one_live_month_lk_controller()


@sales_all_in_one_live_bp.route("/month_lk_without_gst", methods=["GET"])
def get_sales_all_in_one_live_month_lk_without_gst():
    return get_sales_all_in_one_live_month_lk_without_gst_controller()


@sales_all_in_one_live_bp.route("/month_sales_qty", methods=["GET"])
def get_sales_all_in_one_live_month_sales_qty():
    return get_sales_all_in_one_live_month_sales_qty_controller()


@sales_all_in_one_live_bp.route("/month_total_sales", methods=["GET"])
def get_sales_all_in_one_live_month_total_sales():
    return get_sales_all_in_one_live_month_total_sales_controller()

@sales_all_in_one_live_bp.route("/month_gp_lk", methods=["GET"])
def get_sales_all_in_one_live_month_gp_lk():
    return get_sales_all_in_one_live_month_gp_lk_controller()
