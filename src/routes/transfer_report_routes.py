from flask import Blueprint
from src.controllers.transfer_report_controller import *

transfer_report_bp = Blueprint("transferreport", __name__, url_prefix="/transferreport")

@transfer_report_bp.route("/headoffice", methods=["GET"])
def get_transfer_report():
    return get_transfer_report_controller()

@transfer_report_bp.route("/headoffice/overallcount", methods=["GET"])
def get_overall_transfer_report_count():
    return get_overall_transfer_report_count_controller()

# @transfer_report_bp.route("/headoffice/search", methods=["GET"])
# def get_transfer_report_search():
#     return get_transfer_report_search_controller()