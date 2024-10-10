from flask import Blueprint
from src.controllers.test_controller import *

test_bp = Blueprint(
    "test", __name__, url_prefix="/test"
)

@test_bp.route("/testweek", methods=["GET"])
def test_controller_cr():
    return get_test_controller()