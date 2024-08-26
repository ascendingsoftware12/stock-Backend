from flask import Blueprint
from src.controllers.target_monitoring_controller import *

target_monitoring_bp = Blueprint(
    "targetmonitoring", __name__, url_prefix="/targetmonitoring"
)

@target_monitoring_bp.route("/store/<string:storecode>", methods=["GET"])
def get_store_target_monitoring(storecode):
    return get_store_target_monitoring_controller(storecode)

@target_monitoring_bp.route("/store/monthyear/<string:storecode>", methods=["GET"])
def get_store_month_year_based_target_monitoring(storecode):
    return get_store_month_year_based_target_monitoring_controller(storecode)

@target_monitoring_bp.route("headoffice/storesales", methods=["GET"])
def get_head_office_stores_sales():
    return get_head_office_stores_sales_controller()

@target_monitoring_bp.route("headoffice/storetargetachieved", methods=["GET"])
def get_head_office_store_target_achieved_status():
    return get_head_office_store_target_achieved_status_controller()

@target_monitoring_bp.route("headoffice/cm_vs_3mnth", methods=["GET"])
def get_head_office_cm_vs_3mnth_avg_growth():
    return get_head_office_cm_vs_3mnth_avg_growth_controller()

@target_monitoring_bp.route("headoffice/cm_vs_lm", methods=["GET"])
def get_head_office_cm_vs_lm_growth():
    return get_head_office_cm_vs_lm_growth_controller()

@target_monitoring_bp.route("headoffice/dendrogram", methods=["GET"])
def get_head_office_dendrogram():
    return get_head_office_dendrogram_controller()