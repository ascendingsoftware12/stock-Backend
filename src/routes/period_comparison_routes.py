from flask import Blueprint
from src.controllers.period_comparison_controller import*

period_comparison_live_bp = Blueprint(
    "period_comparison", __name__, url_prefix="/period_comparison"
)

# --------------------------------------------------------------------------------------

@period_comparison_live_bp.route("/periodComparisonsales", methods=["GET"])
def period_comparison_PeriodComparison():
    return search_PeriodComparison_controller()

@period_comparison_live_bp.route("/periodComparisonDiscAmt", methods=["GET"])
def period_comparison_PeriodComparison_disc_amt():
    return search_PeriodComparison_DisAmt_controller()

@period_comparison_live_bp.route("/periodComparisonSalesQty", methods=["GET"])
def period_comparison_PeriodComparison_Sales_Qty():
    return search_PeriodComparison_SalesQty_controller()

@period_comparison_live_bp.route("/perioddisComparisonsales", methods=["GET"])
def period_comparison_dis_PeriodComparison():
    return search_PeriodComparison_dis_controller()

@period_comparison_live_bp.route("/periodComparisonsalesstorecode", methods=["GET"])
def period_comparison_store_code_PeriodComparison():
    return search_PeriodComparison_store_code_controller()

@period_comparison_live_bp.route("/periodComparisonaps", methods=["GET"])
def period_comparison_PeriodComparison_asp():
    return search_PeriodComparison_asp_controller()

@period_comparison_live_bp.route("/periodComparisonbranchwiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_branch_wise_Analysis():
    return search_PeriodComparison_branch_wise_Analysis_controller()

@period_comparison_live_bp.route("/periodComparisoncitywiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_city_wise_Analysis():
    return search_PeriodComparison_city_wise_Analysis_controller()

@period_comparison_live_bp.route("/periodComparisonsectionwiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_section_wise_Analysis():
    return search_PeriodComparison_section_wise_Analysis_controller()

@period_comparison_live_bp.route("/periodComparisonitemcategorywiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_itemcategory_wise_Analysis():
    return search_PeriodComparison_itemcategory_wise_Analysis_controller()

@period_comparison_live_bp.route("/periodComparisonproductwiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_product_wise_Analysis():
    return search_PeriodComparison_product_wise_Analysis_controller()


@period_comparison_live_bp.route("/periodComparisonbrandwiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_brand_wise_Analysis():
    return search_PeriodComparison_brand_wise_Analysis_controller()


@period_comparison_live_bp.route("/periodComparisonitemwiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_item_wise_Analysis():
    return search_PeriodComparison_item_wise_Analysis_controller()


@period_comparison_live_bp.route("/periodComparisonpricewiseAnalysis", methods=["GET"])
def period_comparison_PeriodComparison_price_wise_Analysis():
    return search_PeriodComparison_price_wise_Analysis_controller()

@period_comparison_live_bp.route("/periodComparisonallincolumn", methods=["GET"])
def period_comparison_PeriodComparison_all_in_column():
    return get_PeriodComparison_all_in_column_live_controller()