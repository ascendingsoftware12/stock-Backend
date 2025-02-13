from flask import request, jsonify
from sqlalchemy import case, func,and_,text
from src import db
from collections import defaultdict
from src.models.target_achievement_emp_ranking_model import TargetAchievementAnalysisEmpRanking
from src.models.target_achievement_store_ranking_model import TargetAchievementAnalysisStoreRanking
from datetime import datetime
import traceback
import re

def search_targetAchievement_all_metrics_controller():
    try:

        # Retrieve additional filters from request
        store_name = request.args.get('store_name')
        section = request.args.get('section') or 'ALL SECTION' 
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')
        

        # Initialize 
        conditions = []
        def split_values(value):
            return re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', value) if isinstance(value, str) else value
        
        # Apply multiple filters
        if store_name and store_name.strip():
            store_name_list = split_values(store_name)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = split_values(section)
            conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = split_values(tgt_timeline)
            conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = split_values(store_code)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            asm_list = split_values(asm)
            conditions.append(TargetAchievementAnalysisStoreRanking.asm.in_(asm_list))
        # if asm:
        #     conditions.append(TargetAchievementAnalysisStoreRanking.asm == asm)
        # elif store_name and store_name.strip():
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.store_name == store_name.strip())

        # elif section and section.strip():
        #     print(section)
        #     section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
        #     conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.section == section.strip())

        # elif tgt_timeline and tgt_timeline.strip():
        #     tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
        #     conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline == tgt_timeline.strip())
        #     print(tgt_timeline)
        # elif store_code and store_code.strip():
        #     store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
        #     conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.store_code == store_code.strip())

        
        if not isinstance(conditions, list):
            conditions = []
        # Initialize a dictionary to hold the results for each metric
        results = {}

        # Query for Target
        # target_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.target).label('Target')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        
        # target = target_query.scalar() or 0
        # results["Target"] = target

        # # Query for Total Store
        # total_store_query = db.session.query(
        #     func.count(func.distinct(TargetAchievementAnalysisStoreRanking.store_code)).label('total_store')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # total_store = total_store_query.scalar() or 0
        # results["total_store"] = total_store

        # # Query for Total Sales
        # total_sales_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.total_sales).label('total_sales')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # total_sales = total_sales_query.scalar() or 0
        # results["total_sales"] = total_sales

        # # Query for Sales Projection
        # sales_proj_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.sales_proj).label('sales_proj')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # sales_proj = sales_proj_query.scalar() or 0
        # results["sales_proj"] = sales_proj

        # # Query for Total Discount
        # total_disc_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.total_disc).label('total_disc')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # total_disc = total_disc_query.scalar() or 0
        # results["total_disc"] = total_disc

        # # Query for Discount Projection
        # disc_proj_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.disc_proj).label('disc_proj')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # disc_proj = disc_proj_query.scalar() or 0
        # results["disc_proj"] = disc_proj

        # # Query for Target Achievement Percentage
        # target_ach_percentage_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.target_ach_percent).label('target_ach_percentage')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # target_ach_percentage = target_ach_percentage_query.scalar() or 0
        # results["target_ach_percentage"] = target_ach_percentage

        # proj_target_query = db.session.query(
        #     func.sum(TargetAchievementAnalysisStoreRanking.proj_target).label('proj_target')
        # ).join(
        #     TargetAchievementAnalysisEmpRanking,
        #     and_(
        #         TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
        #         TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
        #         TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
        #     )
        # ).filter(*conditions)
        # proj_target = proj_target_query.scalar() or 0
        # results["proj_target"] = proj_target
        
        result = db.session.query(
            TargetAchievementAnalysisEmpRanking.total_sales.label('total_sales'),
            TargetAchievementAnalysisEmpRanking.sales_proj.label('sales_proj'),
            TargetAchievementAnalysisEmpRanking.total_disc.label('total_disc'),
            TargetAchievementAnalysisEmpRanking.disc_proj.label('disc_proj'),
            TargetAchievementAnalysisEmpRanking.target_ach_percent.label('target_ach_percentage'),
            TargetAchievementAnalysisEmpRanking.proj_target.label('proj_target'),
            TargetAchievementAnalysisEmpRanking.section,
            TargetAchievementAnalysisEmpRanking.target.label('Target'),
            func.count(func.distinct(TargetAchievementAnalysisStoreRanking.store_code)).label('total_store')
        ).join(
            TargetAchievementAnalysisStoreRanking,
            and_(
                TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
                TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
                TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
            )
        ).filter(
            *conditions
        ).group_by(
            TargetAchievementAnalysisEmpRanking.total_sales,
            TargetAchievementAnalysisEmpRanking.sales_proj,
            TargetAchievementAnalysisEmpRanking.total_disc,
            TargetAchievementAnalysisEmpRanking.disc_proj,
            TargetAchievementAnalysisEmpRanking.target_ach_percent,
            TargetAchievementAnalysisEmpRanking.target,
            TargetAchievementAnalysisEmpRanking.section,
            TargetAchievementAnalysisEmpRanking.proj_target
        ).all()

        formatted_result = []

        for row in result:            
                if row.section =="ALL SECTION":
                   formatted_result.append({
                "total_sales": row.total_sales,
                "sales_proj": row.sales_proj,
                "total_disc": row.total_disc,
                "disc_proj": row.disc_proj,
                "target_ach_percentage": row.target_ach_percentage,
                "proj_target": row.proj_target,
                "Target": row.Target,
                "total_store": row.total_store
            })

        # Return all the results in a single response
        return jsonify({
            "success": 1,
            "data": formatted_result
        })

    except Exception as e:
        # Rollback the session in case of an error
        db.session.rollback()
        error_message = str(e)

        # Handle specific database disconnection error
        if "MySQL server has gone away" in error_message:
            return search_targetAchievement_all_metrics_controller()  # Retry the function
        else:
            return jsonify({"success": 0, "error": error_message})


#-----------------OverAll Details-------------


def search_targetAchievement_overall_details_controller():
    try:
        store_name = request.args.get('store_name')
        section = request.args.get('section')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            print(store_name)
            print(store_name_list)
            conditions.append(TargetAchievementAnalysisEmpRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else brand
            conditions.append(TargetAchievementAnalysisEmpRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(TargetAchievementAnalysisEmpRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(TargetAchievementAnalysisEmpRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            conditions.append(TargetAchievementAnalysisEmpRanking.emp_name == asm.strip())

        # After adding the conditions, apply them to the query
        section_query = db.session.query(
            TargetAchievementAnalysisEmpRanking.section.label('section'),
            TargetAchievementAnalysisEmpRanking.target.label('TARGET'),
            TargetAchievementAnalysisEmpRanking.total_sales.label('CURRENT_SALES'),
            TargetAchievementAnalysisEmpRanking.total_qty.label('CURRENT_QTY'),
            TargetAchievementAnalysisEmpRanking.sales_proj.label('SALES_PROJECTION'),
            TargetAchievementAnalysisEmpRanking.total_disc.label('CURRENT_DISCOUNT'),
            TargetAchievementAnalysisEmpRanking.disc_proj.label('DISCOUNT_PROJECTION'),
            TargetAchievementAnalysisEmpRanking.avg_value_ach_target.label('AVG_QTY_VALUE'),
            TargetAchievementAnalysisEmpRanking.target_ach_percent.label('TARGET_ACH_PERCENT'),
            TargetAchievementAnalysisEmpRanking.proj_target.label('PROJ_TARGET_ACH'),
           
        ).filter(*conditions)

        section_result = section_query.all()

        formatted_result = []

        for result in section_result:
            if section !="":
              formatted_result.append({
                "Avg_Qty_Value": result.AVG_QTY_VALUE,
                "target_ach_percent": result.TARGET_ACH_PERCENT,
                "Current_Discount": result.CURRENT_DISCOUNT,
                "Current_Qty": result.CURRENT_QTY,
                "Discount_Projection": result.DISCOUNT_PROJECTION,
                "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                "Sales_Projection": result.SALES_PROJECTION,
                "section": result.section,
                "current_Sales": result.CURRENT_SALES,
                "target": result.TARGET
               })
            else:
                if result.section !="ALL SECTION":
                   formatted_result.append({
                "Avg_Qty_Value": result.AVG_QTY_VALUE,
                "target_ach_percent": result.TARGET_ACH_PERCENT,
                "Current_Discount": result.CURRENT_DISCOUNT,
                "Current_Qty": result.CURRENT_QTY,
                "Discount_Projection": result.DISCOUNT_PROJECTION,
                "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                "Sales_Projection": result.SALES_PROJECTION,
                "section": result.section,
                "current_Sales": result.CURRENT_SALES,
                "target": result.TARGET
                 })

        response = {
            "data": formatted_result,
            "success": 1
        }

        return jsonify(response)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the request in case of a MySQL server timeout
            return search_targetAchievement_overall_details_controller()
        else:
            # Return error message for other exceptions
            return jsonify({"success": 0, "error": str(e)})

#------------------Branch Details------------------

def search_targetAchievement_branch_details_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        store_name = request.args.get('store_name')
        section = request.args.get('section')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else brand
            conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            conditions.append(TargetAchievementAnalysisStoreRanking.asm == asm.strip())

        # After adding the conditions, apply them to the query
        section_query = db.session.query(
            TargetAchievementAnalysisStoreRanking.store_name.label('store_name'),
            TargetAchievementAnalysisStoreRanking.section.label('section'),
            TargetAchievementAnalysisStoreRanking.target.label('TARGET'),
            TargetAchievementAnalysisStoreRanking.total_sales.label('CURRENT_SALES'),
            TargetAchievementAnalysisStoreRanking.sales_proj.label('SALES_PROJECTION'),
            TargetAchievementAnalysisStoreRanking.total_disc.label('CURRENT_DISCOUNT'),
            TargetAchievementAnalysisEmpRanking.disc_proj.label('DISCOUNT_PROJECTION'),
            TargetAchievementAnalysisEmpRanking.avg_value_ach_target.label('AVG_QTY_VALUE'),
            TargetAchievementAnalysisEmpRanking.target_ach_percent.label('TARGET_ACH_PERCENT'),
            TargetAchievementAnalysisStoreRanking.proj_target.label('PROJ_TARGET_ACH'),
           
        ).filter(*conditions).group_by(
            TargetAchievementAnalysisStoreRanking.section,
            TargetAchievementAnalysisStoreRanking.store_name
        ).limit(limit).offset(offset)

        final_conditions = " AND ".join(
        str(cond.compile(compile_kwargs={"literal_binds": True})).replace('"', '').replace("store_ranking.", "")
        for cond in conditions
    ) if conditions else ""

        weekly_sales_query = text(f'''
            SELECT 
                STORE_NAME,
                SECTION,
                TARGET,
                TOTAL_SALES,
                SALES_PROJ AS SALES_PROJECTION,
                TOTAL_DISC AS CURRENT_DISCOUNT,
                DISC_PROJ AS DISCOUNT_PROJECTION,
                PROJ_TARGET AS PROJ_TARGET_ACH,
                `target_ach%` AS TARGET_ACH_PERCENT,
                AVG_VALUE_ACH_TARGET AS AVG_QTY_VALUE
            FROM store_ranking
            WHERE 1 = 1
            {f"AND {final_conditions}" if final_conditions else ""}
            GROUP BY STORE_NAME, SECTION
            LIMIT :limit OFFSET :offset;
        ''')

        # Execute the query with limit and offset
        section_result = db.session.execute(weekly_sales_query, {"limit": limit, "offset": offset}).fetchall()

        formatted_result = []

        for result in section_result:
            # Ensure result fields are accessed correctly
            if section:
                formatted_result.append({
                    "Avg_Qty_Value": result.AVG_QTY_VALUE,
                    "target_ach_percent": result.TARGET_ACH_PERCENT,
                    "Current_Discount": result.CURRENT_DISCOUNT,
                    "Discount_Projection": result.DISCOUNT_PROJECTION,
                    "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                    "Sales_Projection": result.SALES_PROJECTION,
                    "section": result.SECTION,
                    "current_Sales": result.TOTAL_SALES,
                    "target": result.TARGET,
                    "store_name": result.STORE_NAME
                })
            else:
                if result.SECTION != "ALL SECTION":
                    formatted_result.append({
                        "Avg_Qty_Value": result.AVG_QTY_VALUE,
                        "target_ach_percent": result.TARGET_ACH_PERCENT,
                        "Current_Discount": result.CURRENT_DISCOUNT,
                        "Discount_Projection": result.DISCOUNT_PROJECTION,
                        "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                        "Sales_Projection": result.SALES_PROJECTION,
                        "section": result.SECTION,
                        "current_Sales": result.TOTAL_SALES,
                        "target": result.TARGET,
                        "store_name": result.STORE_NAME
                    })

        response = {
            "data": formatted_result,
            "success": 1
        }

        return jsonify(response)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the request in case of a MySQL server timeout
            return search_targetAchievement_branch_details_controller()
        else:
            # Return error message for other exceptions
            return jsonify({"success": 0, "error": str(e)})

#--------------------branch Wise Growth--------------

def search_targetAchievement_branch_wise_growth_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        store_name = request.args.get('store_name')
        section = request.args.get('section')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            print(store_name)
            print(store_name_list)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else brand
            conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            conditions.append(TargetAchievementAnalysisStoreRanking.asm == asm.strip())

        # After adding the conditions, apply them to the query
        section_query = db.session.query(
            TargetAchievementAnalysisStoreRanking.store_name.label('store_name'),
            TargetAchievementAnalysisStoreRanking.section.label('section'),
            TargetAchievementAnalysisStoreRanking.cm_lm_growth.label('CMvsLM'),
            TargetAchievementAnalysisStoreRanking.target_lm_ach_percentage.label('target_lm_ach_percentage'),
            TargetAchievementAnalysisStoreRanking.cm_3mnth_growth.label('CMvs3Mnth'),
            TargetAchievementAnalysisStoreRanking.target_mb_ach_percentage.label('target_mb_ach_percentage'),
            TargetAchievementAnalysisStoreRanking.target_2mb_ach_percentage.label('target_2mb_ach_percentage'),
           
        ).filter(*conditions).group_by(
            TargetAchievementAnalysisStoreRanking.section,
            TargetAchievementAnalysisStoreRanking.store_name).limit(limit).offset(offset)

        section_result = section_query.all()

        formatted_result = []

        for result in section_result:
            if section !="":
              formatted_result.append({
                "store_name": result.store_name,
                "section": result.section,
                "CMvsLM": result.CMvsLM,
                "target_lm_ach_percentage": result.target_lm_ach_percentage,
                "CMvs3Mnth": result.CMvs3Mnth,
                "target_mb_ach_percentage": result.target_mb_ach_percentage,
                "target_2mb_ach_percentage": result.target_2mb_ach_percentage,
            
            })
            else:
                if result.section !="ALL SECTION":
                   formatted_result.append({
                "store_name": result.store_name,
                "section": result.section,
                "CMvsLM": result.CMvsLM,
                "target_lm_ach_percentage": result.target_lm_ach_percentage,
                "CMvs3Mnth": result.CMvs3Mnth,
                "target_mb_ach_percentage": result.target_mb_ach_percentage,
                "target_2mb_ach_percentage": result.target_2mb_ach_percentage,
            
            })

        response = {
            "data": formatted_result,
            "success": 1
        }

        return jsonify(response)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the request in case of a MySQL server timeout
            return search_targetAchievement_branch_details_controller()
        else:
            # Return error message for other exceptions
            return jsonify({"success": 0, "error": str(e)})
#---------------------Column-----------------

def search_targetAchievement_common_controller():
    """Builds query conditions based on input parameters."""
    try:

        # Retrieve additional filters from request
        store_name = request.args.get('store_name')
        section = request.args.get('section')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        # Initialize conditions
        conditions = []
        def split_values(value):
            return re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', value) if isinstance(value, str) else value
        
        # Apply multiple filters
        if store_name and store_name.strip():
            store_name_list = split_values(store_name)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = split_values(section)
            conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = split_values(tgt_timeline)
            conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = split_values(store_code)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            asm_list = split_values(asm)
            conditions.append(TargetAchievementAnalysisStoreRanking.asm.in_(asm_list))
 

        # if store_name and store_name.strip():
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.store_name == store_name.strip())

        # elif section and section.strip():
        #     section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
        #     conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.section == section.strip())

        # elif tgt_timeline and tgt_timeline.strip():
        #     tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
        #     conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline == tgt_timeline.strip())

        # elif store_code and store_code.strip():
        #     store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
        #     conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))
        #     # conditions.append(TargetAchievementAnalysisStoreRanking.store_code == store_code.strip())

        # elif asm and asm.strip():
        #     conditions.append(TargetAchievementAnalysisStoreRanking.asm == asm.strip())

        return conditions

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the function if the MySQL connection is lost
            return search_targetAchievement_common_controller()
        else:
            raise e


# def get_targetAchievement_all_in_column_live_controller():
#     """Fetches distinct branch names and sections based on conditions."""
#     try:

#         # Retrieve additional filters from request
#         store_name = request.args.get('store_name')
#         section = request.args.get('section')
#         tgt_timeline = request.args.get('tgt_timeline')
#         store_code = request.args.get('storecode')
#         asm = request.args.get('asm')

#         # Initialize conditions
#         conditions = []

#         if store_name and store_name.strip():
#             store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
#             conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))
#             # conditions.append(TargetAchievementAnalysisStoreRanking.store_name == store_name.strip())

#         elif section and section.strip():
#             section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
#             conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))
#             # conditions.append(TargetAchievementAnalysisStoreRanking.section == section.strip())

#         elif tgt_timeline and tgt_timeline.strip():
#             tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
#             conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))
#             # conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline == tgt_timeline.strip())

#         elif store_code and store_code.strip():
#             store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
#             conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))
#             # conditions.append(TargetAchievementAnalysisStoreRanking.store_code == store_code.strip())

#         elif asm and asm.strip():
#             conditions.append(TargetAchievementAnalysisStoreRanking.asm == asm.strip())
        
#         if not isinstance(conditions, list):
#             conditions = []

#         # Define the query with a join between TargetAchievementAnalysisEmpRanking and TargetAchievementAnalysisStoreRanking
#         query = db.session.query(
#             TargetAchievementAnalysisStoreRanking.store_name,
#             TargetAchievementAnalysisStoreRanking.section,
#             TargetAchievementAnalysisStoreRanking.tgt_timeline
#         ).join(
#             TargetAchievementAnalysisEmpRanking,
#             and_(
#                 TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
#                 TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
#                 TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
#             )
#         ).filter(*conditions)

#         # Get distinct results
#         distinct_data = query.distinct().all()

#         # Process results into a dictionary
#         sales_data = {
#             "store_name": set(),
#             "section": set(),
#             "tgt_timeline": set()
#         }

#         for record in distinct_data:
#             sales_data["store_name"].add(record.store_name)
#             sales_data["section"].add(record.section)
#             sales_data["tgt_timeline"].add(record.tgt_timeline)

#         # Convert sets to lists for JSON serialization
#         sales_data = {key: list(value) for key, value in sales_data.items()}

#         return jsonify(sales_data)

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             # Retry the function if the MySQL connection is lost
#             return get_targetAchievement_all_in_column_live_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})

def get_targetAchievement_all_in_column_live_controller():
    """Fetches distinct branch names and sections based on conditions."""
    try:
        # Retrieve additional filters from request
        store_name = request.args.get('store_name')
        section = request.args.get('section')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        # Initialize conditions
        conditions = []

        # Function to split multiple values from a request parameter
        def split_values(value):
            return re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', value) if isinstance(value, str) else value
        
        # Apply multiple filters
        if store_name and store_name.strip():
            store_name_list = split_values(store_name)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_name.in_(store_name_list))

        if section and section.strip():
            section_list = split_values(section)
            conditions.append(TargetAchievementAnalysisStoreRanking.section.in_(section_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = split_values(tgt_timeline)
            conditions.append(TargetAchievementAnalysisStoreRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = split_values(store_code)
            conditions.append(TargetAchievementAnalysisStoreRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            asm_list = split_values(asm)
            conditions.append(TargetAchievementAnalysisStoreRanking.asm.in_(asm_list))
        
        # Define the query with a join
        query = db.session.query(
            TargetAchievementAnalysisStoreRanking.store_name,
            TargetAchievementAnalysisStoreRanking.section,
            TargetAchievementAnalysisStoreRanking.tgt_timeline
        ).join(
            TargetAchievementAnalysisEmpRanking,
            and_(
                TargetAchievementAnalysisEmpRanking.tgt_period == TargetAchievementAnalysisStoreRanking.tgt_period,
                TargetAchievementAnalysisEmpRanking.emp_name == TargetAchievementAnalysisStoreRanking.asm,
                TargetAchievementAnalysisEmpRanking.section == TargetAchievementAnalysisStoreRanking.section
            )
        ).filter(*conditions)

        # Get distinct results
        distinct_data = query.distinct().all()

        # Process results into a dictionary
        sales_data = {
            "store_name": set(),
            "section": set(),
            "tgt_timeline": set()
        }

        for record in distinct_data:
          if record.section != "ALL SECTION":
            sales_data["store_name"].add(record.store_name)
            sales_data["section"].add(record.section)
            sales_data["tgt_timeline"].add(record.tgt_timeline)

        # Convert sets to lists for JSON serialization
        sales_data = {key: list(value) for key, value in sales_data.items()}

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the function if the MySQL connection is lost
            return get_targetAchievement_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})