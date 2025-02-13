from flask import request, jsonify
from sqlalchemy import case, func,and_
from src import db
from collections import defaultdict
from src.models.brand_achievement_brand_emp_ranking_model import BrandAchievementEmpRanking
from src.models.brand_achievement_brand_ranking_model import BrandAchievementBrandRanking
from datetime import datetime
import traceback
import re


def search_branchAchievement_common_controller():
    """Builds query conditions based on input parameters."""
    try:
        store_name = request.args.get('store_name')
        brand = request.args.get('brand')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(BrandAchievementBrandRanking.store_name.in_(store_name_list))
            # conditions.append(BrandAchievementBrandRanking.store_name == store_name.strip())

        elif brand and brand.strip():
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(BrandAchievementBrandRanking.brand.in_(brand_list))
            # conditions.append(BrandAchievementBrandRanking.brand == brand.strip())
        
        elif tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(BrandAchievementBrandRanking.tgt_timeline.in_(tgt_timeline_list))
            # conditions.append(BrandAchievementBrandRanking.tgt_timeline == tgt_timeline.strip())
        
        elif store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(BrandAchievementBrandRanking.store_code.in_(store_code_list))
            # conditions.append(BrandAchievementBrandRanking.store_code == store_code.strip())
        
        elif asm and asm.strip():
            conditions.append(BrandAchievementBrandRanking.asm == asm.strip())

        return conditions

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the function if the MySQL connection is lost
            return search_branchAchievement_common_controller()
        else:
            raise e



def get_brandAchievement_all_in_column_live_controller():
    """Fetches distinct branch names and sections based on conditions."""
    try:
        store_name = request.args.get('store_name')
        brand = request.args.get('brand')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(BrandAchievementBrandRanking.store_name.in_(store_name_list))
            # conditions.append(BrandAchievementBrandRanking.store_name == store_name.strip())

        elif brand and brand.strip():
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(BrandAchievementBrandRanking.brand.in_(brand_list))
            # conditions.append(BrandAchievementBrandRanking.brand == brand.strip())
        
        elif tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(BrandAchievementBrandRanking.tgt_timeline.in_(tgt_timeline_list))
            # conditions.append(BrandAchievementBrandRanking.tgt_timeline == tgt_timeline.strip())
        
        elif store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(BrandAchievementBrandRanking.store_code.in_(store_code_list))
            # conditions.append(BrandAchievementBrandRanking.store_code == store_code.strip())
        
        elif asm and asm.strip():
            conditions.append(BrandAchievementBrandRanking.asm == asm.strip())

        # Define the query with selected columns
        query = db.session.query(
            BrandAchievementBrandRanking.store_name,
            BrandAchievementBrandRanking.brand,
            BrandAchievementBrandRanking.tgt_timeline
        ).join(
            BrandAchievementEmpRanking,
            and_(
                BrandAchievementBrandRanking.tgt_timeline == BrandAchievementEmpRanking.tgt_timeline,
                BrandAchievementBrandRanking.brand == BrandAchievementEmpRanking.brand,
                BrandAchievementBrandRanking.asm == BrandAchievementEmpRanking.emp_name,
                case(
                    (and_(
                        BrandAchievementEmpRanking.tgt_start_slab == 1,
                        BrandAchievementEmpRanking.tgt_end_slab == 49999
                    ), "Below 50K"),
                    (and_(
                        BrandAchievementEmpRanking.tgt_start_slab == 50000,
                        BrandAchievementEmpRanking.tgt_end_slab == 1000000
                    ), "Above 50K"),
                    else_="BLANK"
                ) == case(
                    (and_(
                        BrandAchievementBrandRanking.tgt_start_slab == 1,
                        BrandAchievementBrandRanking.tgt_end_slab == 49999
                    ), "Below 50K"),
                    (and_(
                        BrandAchievementBrandRanking.tgt_start_slab == 50000,
                        BrandAchievementBrandRanking.tgt_end_slab == 1000000
                    ), "Above 50K"),
                    else_="BLANK"
                )
            )
        ).filter(*conditions)

        # Get distinct results
        distinct_data = query.distinct().all()

        # Process results into a dictionary
        sales_data = {
            "STORE_NAME": set(),
            "BRAND": set(),
            "TGT_TIMELINE": set()
        }

        for record in distinct_data:
            sales_data["STORE_NAME"].add(record.store_name)
            sales_data["BRAND"].add(record.brand)
            sales_data["TGT_TIMELINE"].add(record.tgt_timeline)

        # Convert sets to lists for JSON serialization
        sales_data = {key: list(value) for key, value in sales_data.items()}

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the function if the MySQL connection is lost
            return get_brandAchievement_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

#------------------------------Summary--------------------------

def search_brandAchievement_summary_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        store_name = request.args.get('store_name')
        brand = request.args.get('brand')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            print(store_name)
            print(store_name_list)
            conditions.append(BrandAchievementEmpRanking.store_name.in_(store_name_list))

        if brand and brand.strip():
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(BrandAchievementEmpRanking.brand.in_(brand_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(BrandAchievementEmpRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(BrandAchievementEmpRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            conditions.append(BrandAchievementEmpRanking.emp_name == asm.strip())

        # After adding the conditions, apply them to the query
        section_query = db.session.query(
            BrandAchievementEmpRanking.brand.label('brand'),
            BrandAchievementEmpRanking.target.label('TARGET'),
            BrandAchievementEmpRanking.total_sales.label('CURRENT_SALES'),
            BrandAchievementEmpRanking.total_qty.label('CURRENT_QTY'),
            BrandAchievementEmpRanking.sales_proj.label('SALES_PROJECTION'),
            BrandAchievementEmpRanking.total_disc.label('CURRENT_DISCOUNT'),
            BrandAchievementEmpRanking.disc_proj.label('DISCOUNT_PROJECTION'),
            BrandAchievementEmpRanking.avg_value_ach_target.label('AVG_QTY_VALUE'),
            BrandAchievementEmpRanking.target_ach_percent.label('CUR_TARGET_ACH'),
            BrandAchievementEmpRanking.proj_target.label('PROJ_TARGET_ACH'),
            case(
                (and_(BrandAchievementEmpRanking.tgt_start_slab == 1, BrandAchievementEmpRanking.tgt_end_slab == 49999), "Below 50K"),
                (and_(BrandAchievementEmpRanking.tgt_start_slab == 50000, BrandAchievementEmpRanking.tgt_end_slab == 1000000), "Above 50K"),
                else_=None
            ).label("SLAB")
        ).filter(*conditions).group_by(
            BrandAchievementEmpRanking.brand,
            BrandAchievementEmpRanking.tgt_start_slab,
            BrandAchievementEmpRanking.tgt_end_slab
        ).limit(limit).offset(offset)

        section_result = section_query.all()

        formatted_result = [
            {
                "Avg_Qty_Value": round(result.AVG_QTY_VALUE, 2),
                "Cur_Target_Ach": result.CUR_TARGET_ACH,
                "Current_Discount": result.CURRENT_DISCOUNT,
                "Current_Qty": result.CURRENT_QTY,
                "Discount_Projection": result.DISCOUNT_PROJECTION,
                "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                "Sales_Projection": result.SALES_PROJECTION,
                "Slab": result.SLAB,
                "brand": result.brand,
                "current_Sales": result.CURRENT_SALES,
                "target": result.TARGET
            }
            for result in section_result
        ]

        response = {
            "data": formatted_result,
            "success": 1
        }

        return jsonify(response)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the request in case of a MySQL server timeout
            return search_brandAchievement_summary_controller()
        else:
            # Return error message for other exceptions
            return jsonify({"success": 0, "error": str(e)})

#-------------------------------Branch wise details----------------

def search_brandAchievement_branch_Wise_details_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        store_name = request.args.get('store_name')
        brand = request.args.get('brand')
        tgt_timeline = request.args.get('tgt_timeline')
        store_code = request.args.get('storecode')
        asm = request.args.get('asm')

        conditions = []

        if store_name and store_name.strip():
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            print(store_name)
            print(store_name_list)
            conditions.append(BrandAchievementBrandRanking.store_name.in_(store_name_list))

        if brand and brand.strip():
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(BrandAchievementBrandRanking.brand.in_(brand_list))

        if tgt_timeline and tgt_timeline.strip():
            tgt_timeline_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', tgt_timeline) if isinstance(tgt_timeline, str) else tgt_timeline
            conditions.append(BrandAchievementBrandRanking.tgt_timeline.in_(tgt_timeline_list))

        if store_code and store_code.strip():
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(BrandAchievementBrandRanking.store_code.in_(store_code_list))

        if asm and asm.strip():
            conditions.append(BrandAchievementBrandRanking.asm == asm.strip())

        # After adding the conditions, apply them to the query
        section_query = db.session.query(
            BrandAchievementBrandRanking.store_name.label('store_name'),
            
            BrandAchievementBrandRanking.brand.label('brand'),
            BrandAchievementBrandRanking.target.label('TARGET'),
            BrandAchievementBrandRanking.total_sales.label('CURRENT_SALES'),
            BrandAchievementBrandRanking.total_qty.label('CURRENT_QTY'),
            BrandAchievementBrandRanking.sales_proj.label('SALES_PROJECTION'),
            BrandAchievementBrandRanking.total_disc.label('CURRENT_DISCOUNT'),
            BrandAchievementBrandRanking.disc_proj.label('DISCOUNT_PROJECTION'),
            BrandAchievementBrandRanking.avg_value_ach_target.label('AVG_QTY_VALUE'),
            BrandAchievementBrandRanking.target_ach_percent.label('TARGET_ACH_PERCENT'),
            BrandAchievementBrandRanking.proj_target.label('PROJ_TARGET_ACH'),
            case(
                (and_(BrandAchievementBrandRanking.tgt_start_slab == 1, BrandAchievementBrandRanking.tgt_end_slab == 49999), "Below 50K"),
                (and_(BrandAchievementBrandRanking.tgt_start_slab == 50000, BrandAchievementBrandRanking.tgt_end_slab == 1000000), "Above 50K"),
                else_=None
            ).label("SLAB")
        ).filter(*conditions).group_by(
            BrandAchievementBrandRanking.brand,
            BrandAchievementBrandRanking.store_name,
            BrandAchievementBrandRanking.tgt_start_slab,
            BrandAchievementBrandRanking.tgt_end_slab
        ).limit(limit).offset(offset)

        section_result = section_query.all()

        formatted_result = [
            {
                "Avg_Qty_Value": result.AVG_QTY_VALUE,
                "target_ach_percent": result.TARGET_ACH_PERCENT,
                "Current_Discount": result.CURRENT_DISCOUNT,
                "Current_Qty": result.CURRENT_QTY,
                "Discount_Projection": result.DISCOUNT_PROJECTION,
                "Proj_Target_Ach": result.PROJ_TARGET_ACH,
                "Sales_Projection": result.SALES_PROJECTION,
                "Slab": result.SLAB,
                "store_name": result.store_name,
                "current_Sales": result.CURRENT_SALES,
                
                "target": result.TARGET,
                "brand": result.brand
            }
            for result in section_result
        ]

        response = {
            "data": formatted_result,
            "success": 1
        }

        return jsonify(response)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry the request in case of a MySQL server timeout
            return search_brandAchievement_branch_Wise_details_controller()
        else:
            # Return error message for other exceptions
            return jsonify({"success": 0, "error": str(e)})

