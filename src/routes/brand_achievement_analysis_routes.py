from flask import Blueprint
from src.controllers.brand_achievement_analysis_controller import*

brand_achievement_analysis_live_bp = Blueprint(
    "brand_achievement_analysis", __name__, url_prefix="/brand_achievement_analysis"
)

@brand_achievement_analysis_live_bp.route("/brandachivementcolumn",methods=["GET"])
def brand_achivement_analysis_column():
    return get_brandAchievement_all_in_column_live_controller()

@brand_achievement_analysis_live_bp.route("/brandachivementsummary",methods=["GET"])
def brand_achivement_analysis_summary():
    return search_brandAchievement_summary_controller()

@brand_achievement_analysis_live_bp.route("/brandachivementbranchwisedetails",methods=["GET"])
def brand_achivement_analysis_branch_wise_details():
    return search_brandAchievement_branch_Wise_details_controller()
