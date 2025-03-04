from flask import Blueprint
from src.controllers.sales_analysis_kore_controller import*

sales_analysis_kore_live_bp = Blueprint(
    "sales_analysis_kore", __name__, url_prefix="/sales_analysis_kore"
)

@sales_analysis_kore_live_bp.route("/salesAnalysisKoreColumn", methods=["GET"])
def sales_analysis_kore_all_in_column():
    return get_SalesAnlaysis_kore_all_in_column_live_controller()

@sales_analysis_kore_live_bp.route("/salesAnalysisKoreSalestype", methods=["GET"])
def sales_analysis_kore_Sales_Type():
    return search_SalesContributuion_kore_sales_type_controller()


@sales_analysis_kore_live_bp.route("/salesAnalysisKoreSales", methods=["GET"])
def sales_analysis_kore_Sales():
    return search_SalesContribution_kore_sales_controller()

@sales_analysis_kore_live_bp.route("/salesAnalysisKoreSection", methods=["GET"])
def sales_analysis_kore_Section():
    return search_SalesContributuion_kore_section_controller()





@sales_analysis_kore_live_bp.route("/salesAnalysisKoreBrandSales", methods=["GET"])
def sales_analysis_kore_Brand_Sales():
    return search_SalesContributuion_kore_brand_sales_controller()

@sales_analysis_kore_live_bp.route("/salesAnalysisKoreItemSales", methods=["GET"])
def sales_analysis_kore_Item_Sales():
    return search_SalesContributuion_kore_item_sales_controller()





@sales_analysis_kore_live_bp.route("/DiscountAnalysisKoreSection", methods=["GET"])
def discount_analysis_kore_section():
    return search_Discount_Analysis_kore_section_controller()


@sales_analysis_kore_live_bp.route("/DiscountAnalysisKoreBrand", methods=["GET"])
def discount_analysis_kore_brand():
    return search_Discount_Analysis_kore_brand_controller()

@sales_analysis_kore_live_bp.route("/DiscountAnalysisKoreModelNo", methods=["GET"])
def discount_analysis_kore_model_no():
    return search_Discount_Analysis_kore_model_no_controller()

@sales_analysis_kore_live_bp.route("/DiscountAnalysisKoreDiscount", methods=["GET"])
def discount_analysis_Kore_discount():
    return search_Discount_Analysis_kore_discount_controller()


@sales_analysis_kore_live_bp.route("/DiscountAnalysisKoreDiscountPercentage", methods=["GET"])
def discount_analysis_kore_discount_percentage():
    return search_Discount_Analysis_kore_discount_Percentage_controller()




