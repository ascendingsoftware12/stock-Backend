from flask import Blueprint
from src.controllers.sales_all_in_one_live_controller import *

sales_all_in_one_live_bp = Blueprint(
    "sales_all_in_one_live", __name__, url_prefix="/sales_all_in_one_live"
)

# -----------------------------------------------------
# main routes
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/monthly/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_monthly(factor):
    return get_sales_all_in_one_live_monthly_controller(factor)


@sales_all_in_one_live_bp.route("/ytd/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_ytd(factor):
    return get_sales_all_in_one_live_ytd_controller(factor)


@sales_all_in_one_live_bp.route("/weekly_analysis/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_weekly_analysis(factor):
    return get_sales_all_in_one_live_weekly_analysis_controller(factor)


@sales_all_in_one_live_bp.route("/day_analysis/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_day_analysis(factor):
    return get_sales_all_in_one_live_day_analysis_controller(factor)


@sales_all_in_one_live_bp.route("/product_dimension/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_product_dimension(factor):
    return get_sales_all_in_one_live_product_dimension_controller(factor)


@sales_all_in_one_live_bp.route("/brand_dimension/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_brand_dimension(factor):
    return get_sales_all_in_one_live_brand_dimension_controller(factor)


@sales_all_in_one_live_bp.route("/item_dimension/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_item_dimension(factor):
    return get_sales_all_in_one_live_item_dimension_controller(factor)


@sales_all_in_one_live_bp.route("/price_breakup_one/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_price_breakup_one(factor):
    return get_sales_all_in_one_live_price_breakup_one_controller(factor)


@sales_all_in_one_live_bp.route("/price_breakup_two/<string:factor>", methods=["GET"])
def get_sales_all_in_one_live_price_breakup_two(factor):
    return get_sales_all_in_one_live_price_breakup_two_controller(factor)


# -----------------------------------------------------

# -----------------------------------------------------
# Get all
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/", methods=["GET"])
def get_sales_all_in_one_live():
    return get_sales_all_in_one_live_controller()

# -----------------------------------------------------
# Search Utils 
# -----------------------------------------------------

@sales_all_in_one_live_bp.route("/unique_srn_flags", methods=["GET"])
def get_unique_srn_flags():
    return get_unique_srn_flags_controller()

@sales_all_in_one_live_bp.route("/unique_invoice_dates", methods=["GET"])
def get_unique_invoice_dates():
    return get_unique_invoice_dates_controller()

@sales_all_in_one_live_bp.route("/unique_sale_types", methods=["GET"])
def get_unique_sale_types():
    return get_unique_sale_types_controller()

@sales_all_in_one_live_bp.route("/itemsdesc_brand_model_and_section", methods=["GET"])
def get_itemsdesc_brand_model_and_section():
    return get_itemsdesc_brand_model_and_section_controller()



# -----------------------------------------------------

# -----------------------------------------------------
# ytd
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/ytd_cr", methods=["GET"])
def get_sales_all_in_one_live_ytd_cr():
    return get_sales_all_in_one_live_ytd_cr_controller()


# -----------------------------------------------------
# month
# -----------------------------------------------------

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


# -----------------------------------------------------
# weekly analysis
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/weekly_analysis_cr", methods=["GET"])
def get_sales_all_in_one_live_weekly_analysis_cr():
    return get_sales_all_in_one_live_weekly_analysis_cr_controller()

# -----------------------------------------------------
# day analysis
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/day_analysis_cr", methods=["GET"])
def get_sales_all_in_one_live_day_analysis_cr():
    return get_sales_all_in_one_live_day_analysis_cr_controller()


# -----------------------------------------------------
# product dimension
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/product_dimension_cr", methods=["GET"])
def get_sales_all_in_one_live_product_dimension_cr():
    return get_sales_all_in_one_live_product_dimension_cr_controller()


# -----------------------------------------------------
# brand name
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/brand_dimension_cr", methods=["GET"])
def get_sales_all_in_one_live_brand_name_cr():
    return get_sales_all_in_one_live_brand_dimension_cr_controller()


# -----------------------------------------------------
# item dimension
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/item_dimension_cr", methods=["GET"])
def get_sales_all_in_one_live_item_dimension_cr():
    return get_sales_all_in_one_live_item_dimension_cr_controller()


# -----------------------------------------------------
# price breakup 1
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/price_breakup_one_cr", methods=["GET"])
def get_sales_all_in_one_live_price_breakup_one_cr():
    return get_sales_all_in_one_live_price_breakup_one_cr_controller()


# -----------------------------------------------------
# price breakup 2
# -----------------------------------------------------


@sales_all_in_one_live_bp.route("/price_breakup_two_cr", methods=["GET"])
def get_sales_all_in_one_live_price_breakup_two_cr():
    return get_sales_all_in_one_live_price_breakup_two_cr_controller()
