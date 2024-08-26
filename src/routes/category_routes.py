from flask import Blueprint
from src.controllers.category_controller import *

category_bp = Blueprint("category", __name__)

@category_bp.route("/section", methods=["GET"], strict_slashes=False)
def get_section():
    return get_section_controller()

@category_bp.route("/brandnames", methods=["GET"], strict_slashes=False)
def get_brand_names():
    return get_brand_name_controller()

@category_bp.route("/storenames", methods=["GET"], strict_slashes=False)
def get_store_names():
    return get_store_name_controller()

@category_bp.route("/modelnames", methods=["GET"], strict_slashes=False)
def get_item_names():
    return get_item_name_controller()

@category_bp.route("/getmodelbrandstore", methods=["GET"], strict_slashes=False)
def get_store_brand_model_name():
    return get_store_brand_model_name_controller()

@category_bp.route("/statenames", methods=["GET"], strict_slashes=False)
def get_state_name():
    return get_state_name_controller()

@category_bp.route("/getfromstoretostorebrandmodel", methods=["GET"], strict_slashes=False)
def get_from_store_to_store_brand_model():
    return get_from_store_to_store_brand_model_controller()