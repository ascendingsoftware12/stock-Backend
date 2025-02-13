from flask import Blueprint
from src.controllers.stock_all_in_one_summary_controller import*

stock_analysis_live_bp = Blueprint(
    "stock_analysis", __name__, url_prefix="/stock_analysis"
)

@stock_analysis_live_bp.route("/stockAnalysisColumn", methods=["GET"])
def stockAnalysis_all_in_column():
    return get_StockAnlaysis_all_in_column_live_controller()

@stock_analysis_live_bp.route("/stockAnalysisQuantity", methods=["GET"])
def StockAnalysis_quantity():
    return search_StockAnalysis_quantity_controller()

@stock_analysis_live_bp.route("/stockAnalysisSellingPrice", methods=["GET"])
def StockAnalysis_quabity():
    return search_StockAnalysis_Selling_Price_controller()

@stock_analysis_live_bp.route("/stockAnalysisNoOfBrands", methods=["GET"])
def StockAnalysis_no_of_brands():
    return search_StockAnalysis_No_Of_Brands_controller()

@stock_analysis_live_bp.route("/stockAnalysisNoOfItems", methods=["GET"])
def StockAnalysis_no_of_items():
    return search_StockAnalysis_No_Of_Items_controller()

@stock_analysis_live_bp.route("/stockAnalysisAgeing", methods=["GET"])
def StockAnalysis_ageing():
    return search_StockAnalysis_Ageing_controller()

@stock_analysis_live_bp.route("/stockAnalysisHoldingCost", methods=["GET"])
def StockAnalysis_holding_cost():
    return search_StockAnalysis_Holding_Cost_controller()

@stock_analysis_live_bp.route("/stockAnalysisBranch", methods=["GET"])
def StockAnalysis_branch():
    return search_StockAnalysis_Branch_controller()

@stock_analysis_live_bp.route("/stockAnalysisCity", methods=["GET"])
def StockAnalysis_city():
    return search_StockAnalysis_City_controller()

@stock_analysis_live_bp.route("/stockAnalysisSection", methods=["GET"])
def StockAnalysis_section():
    return search_StockAnalysis_section_controller()

@stock_analysis_live_bp.route("/stockAnalysisItemCategory", methods=["GET"])
def StockAnalysis_item_category():
    return search_StockAnalysis_Item_Category_controller()

@stock_analysis_live_bp.route("/stockAnalysisProduct", methods=["GET"])
def StockAnalysis_product():
    return search_StockAnalysis_Product_controller()

@stock_analysis_live_bp.route("/stockAnalysisBrand", methods=["GET"])
def StockAnalysis_brand():
    return search_StockAnalysis_brand_controller()

@stock_analysis_live_bp.route("/stockAnalysisModelNo", methods=["GET"])
def StockAnalysis_modelno():
    return search_StockAnalysis_ModelNo_controller()

@stock_analysis_live_bp.route("/stockAnalysisItem", methods=["GET"])
def StockAnalysis_item_name():
    return search_StockAnalysis_Item_controller()

@stock_analysis_live_bp.route("/stockAnalysisOverAllBucket", methods=["GET"])
def StockAnalysis_overallbucket():
    return search_StockAnalysis_Overall_Bucket_controller()

@stock_analysis_live_bp.route("/stockAnalysisPriceBucket", methods=["GET"])
def StockAnalysis_pricebucket():
    return search_StockAnalysis_Price_Bucket_controller()

@stock_analysis_live_bp.route("/table_modificatio", methods=["GET"])
def StockAnalysis_table_modificatio():
    return get_stock_table_modification_date_and_time_controller()





