from flask import Blueprint
from src.controllers.transfer_receive_summary_controller import *

transfer_receive_summary_bp = Blueprint("transferreceivesummary", __name__, url_prefix="/transferreceivesummary")

@transfer_receive_summary_bp.route("/<string:storecode>", methods=["GET"])
def get_tranfer_receive_summary(storecode):
    return get_tranfer_receive_summary_controller(storecode)

@transfer_receive_summary_bp.route("/", methods=["PUT"])
def update_tranfer_receive_summary():
    return update_tranfer_receive_summary_controller()
