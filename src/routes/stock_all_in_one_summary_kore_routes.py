from flask import Blueprint
from src.controllers.stock_all_in_one_summary_kore_controller import*

stock_analysis_kore_live_bp = Blueprint(
    "stock_analysis_kore", __name__, url_prefix="/stock_analysis_kore"
)

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreColumn", methods=["GET"])
def stockAnalysis_kore_all_in_column():
    return get_StockAnlaysis_kore_all_in_column_live_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreQuantity", methods=["GET"])
def StockAnalysis_kore_quantity():
    return search_StockAnalysis_kore_quantity_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreSellingPrice", methods=["GET"])
def StockAnalysis_kore_quabity():
    return search_StockAnalysis_kore_Selling_Price_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreNoOfBrands", methods=["GET"])
def StockAnalysis_kore_no_of_brands():
    return search_StockAnalysis_kore_No_Of_Brands_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreNoOfItems", methods=["GET"])
def StockAnalysis_kore_no_of_items():
    return search_StockAnalysis_kore_No_Of_Items_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreAgeing", methods=["GET"])
def StockAnalysis_kore_ageing():
    return search_StockAnalysis_kore_Ageing_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreHoldingCost", methods=["GET"])
def StockAnalysis_kore_holding_cost():
    return search_StockAnalysis_kore_Holding_Cost_controller()



@stock_analysis_kore_live_bp.route("/stockAnalysisKoreSection", methods=["GET"])
def StockAnalysis_kore_section():
    return search_StockAnalysis_kore_section_controller()



@stock_analysis_kore_live_bp.route("/stockAnalysisKoreBrand", methods=["GET"])
def StockAnalysis_Kore_brand():
    return search_StockAnalysis_kore_brand_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreModelNo", methods=["GET"])
def StockAnalysis_kore_modelno():
    return search_StockAnalysis_kore_ModelNo_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreItem", methods=["GET"])
def StockAnalysis_kore_item_name():
    return search_StockAnalysis_kore_Item_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisKoreOverAllBucket", methods=["GET"])
def StockAnalysis_kore_overallbucket():
    return search_StockAnalysis_kore_Overall_Bucket_controller()

@stock_analysis_kore_live_bp.route("/stockAnalysisPriceBucket", methods=["GET"])
def StockAnalysis_pricebucket():
    return search_StockAnalysis_kore_Price_Bucket_controller()

@stock_analysis_kore_live_bp.route("/kore_table_modificatio", methods=["GET"])
def StockAnalysis_table_kore_modificatio():
    return get_stock_table_kore_modification_date_and_time_controller()







