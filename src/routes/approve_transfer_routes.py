from flask import Blueprint
from src.controllers.approve_transfer_controller import *
from src.utils.jwt_token_utils import token_required

approve_transfer_bp = Blueprint("approvetransfer", __name__, url_prefix="/approvetransfer")

@approve_transfer_bp.route("/", methods=["GET"])
def get_approve_transfer():   
    return get_approve_transfer_controller()

@approve_transfer_bp.route("/",methods=['PUT'])
def update_approve_transfer():
    return update_approve_transfer_controller()
