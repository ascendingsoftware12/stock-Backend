from flask import Blueprint
from src.controllers.procurement_controller import *

procurement_bp = Blueprint("procurement", __name__, url_prefix="/procurement")

@procurement_bp.route("/notapproved", methods=["GET"])
def get_not_approved_procurement():
    return get_not_approved_procurement_controller()

@procurement_bp.route("/approved", methods=["GET"])
def get_approved_procurement():
    return get_approved_procurement_controller()

@procurement_bp.route("/save", methods=["POST"])
def save_procurement():
    return save_procurement_controller()

@procurement_bp.route("/savefinalpo", methods=["POST"])
def final_po_save_procurement():
    return final_po_save_procurement_controller()

@procurement_bp.route("/generatepo", methods=["PUT"])
def update_projection_procurement():
    return update_projection_procurement_controller()

@procurement_bp.route("/approvedexcel", methods=["GET"])
def approved_export_attaced_excel_procurement():
    return approved_export_attaced_excel_procurement__controller()

@procurement_bp.route("/notapprovedexcel", methods=["GET"])
def not_approved_export_attaced_excel_procurement():
    return not_approved_export_attaced_excel_procurement__controller()