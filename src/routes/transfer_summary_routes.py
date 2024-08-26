from flask import Blueprint
from src.controllers.transfer_summary_controller import *

transfer_summary_bp = Blueprint(
    "transfersummary", __name__, url_prefix="/transfersummary"
)


@transfer_summary_bp.route("/<string:storecode>", methods=["GET"])
def get_tranfer_summary(storecode):
    return get_tranfer_summary_controller(storecode)


@transfer_summary_bp.route("/", methods=["PUT"])
def update_tranfer_summary():
    return update_tranfer_summary_controller()
