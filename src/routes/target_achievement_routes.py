from flask import Blueprint
from src.controllers.target_achievement_analysis_controller import*

target_achievement_analysis_live_bp = Blueprint(
    "target_achievement_analysis", __name__, url_prefix="/target_achievement_analysis"
)

@target_achievement_analysis_live_bp.route("/targetachievementTarget", methods=["GET"])
def target_achievement_analysis_target():
    return search_targetAchievement_all_metrics_controller()

@target_achievement_analysis_live_bp.route("/targetachievementOverallDetails", methods=["GET"])
def search_SalesContributuion_overall_controller():
    return search_targetAchievement_overall_details_controller()

@target_achievement_analysis_live_bp.route("/targetachievementBranchWiseDetails", methods=["GET"])
def search_SalesContributuion_branch_details_controller():
    return search_targetAchievement_branch_details_controller()

@target_achievement_analysis_live_bp.route("/targetachievementBranchWiseGrowth", methods=["GET"])
def search_SalesContributuion_branch_wise_details_controller():
    return search_targetAchievement_branch_wise_growth_controller()

@target_achievement_analysis_live_bp.route("/targetachievementColumn", methods=["GET"])
def search_SalesContributuion_column_controller():
    return get_targetAchievement_all_in_column_live_controller()
