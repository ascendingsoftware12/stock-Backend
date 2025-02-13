from flask import Blueprint
from src.controllers.sales_analysis_controller import*

sales_analysis_live_bp = Blueprint(
    "sales_analysis", __name__, url_prefix="/sales_analysis"
)

@sales_analysis_live_bp.route("/salesAnalysisColumn", methods=["GET"])
def sales_analysis_all_in_column():
    return get_SalesAnlaysis_all_in_column_live_controller()

@sales_analysis_live_bp.route("/salesAnalysisSalestype", methods=["GET"])
def sales_analysis_Sales_Type():
    return search_SalesContributuion_sales_type_controller()


@sales_analysis_live_bp.route("/salesAnalysisSales", methods=["GET"])
def sales_analysis_Sales():
    return search_SalesContribution_sales_controller()

@sales_analysis_live_bp.route("/salesAnalysisSection", methods=["GET"])
def sales_analysis_Section():
    return search_SalesContributuion_section_controller()

@sales_analysis_live_bp.route("/salesAnalysisitemCategory", methods=["GET"])
def sales_analysis_Item_Category():
    return search_SalesContributuion_item_category_controller()

@sales_analysis_live_bp.route("/salesAnalysisProduct", methods=["GET"])
def sales_analysis_Product():
    return search_SalesContributuion_product_controller()

@sales_analysis_live_bp.route("/salesAnalysisBranch", methods=["GET"])
def sales_analysis_Branch():
    return search_SalesContributuion_branch_controller()

@sales_analysis_live_bp.route("/salesAnalysisCity", methods=["GET"])
def sales_analysis_City():
    return search_SalesContributuion_city_controller()

@sales_analysis_live_bp.route("/salesAnalysisBrandSales", methods=["GET"])
def sales_analysis_Brand_Sales():
    return search_SalesContributuion_brand_sales_controller()

@sales_analysis_live_bp.route("/salesAnalysisItemSales", methods=["GET"])
def sales_analysis_Item_Sales():
    return search_SalesContributuion_item_sales_controller()

@sales_analysis_live_bp.route("/DiscountAnalysisBranch", methods=["GET"])
def discount_analysis_branch():
    return search_Discount_Analysis_branch_controller()

@sales_analysis_live_bp.route("/DiscountAnalysisCity", methods=["GET"])
def discount_analysis_city():
    return search_Discount_Analysis_city_controller()

@sales_analysis_live_bp.route("/DiscountAnalysisSection", methods=["GET"])
def discount_analysis_section():
    return search_Discount_Analysis_section_controller()


@sales_analysis_live_bp.route("/DiscountAnalysisBrand", methods=["GET"])
def discount_analysis_brand():
    return search_Discount_Analysis_brand_controller()

@sales_analysis_live_bp.route("/DiscountAnalysisModelNo", methods=["GET"])
def discount_analysis_model_no():
    return search_Discount_Analysis_model_no_controller()

@sales_analysis_live_bp.route("/DiscountAnalysisDiscount", methods=["GET"])
def discount_analysis_discount():
    return search_Discount_Analysis_discount_controller()


@sales_analysis_live_bp.route("/DiscountAnalysisDiscountPercentage", methods=["GET"])
def discount_analysis_discount_percentage():
    return search_Discount_Analysis_discount_Percentage_controller()




