from flask import request, jsonify
from src import db
from src.models.general_branch_ranking_model import GeneralBranchRankingModel
from sqlalchemy.sql import func, case, and_


def get_store_target_monitoring_controller(store_code):
    try:
        sections = ["MOBILE", "ACCESSORIES", "INSURANCE", "WATCH"]
        result = {section.lower(): get_store_section_data(store_code, section) for section in sections}

        return jsonify(result)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_store_target_monitoring_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)
    
def get_store_section_data(store_code, section):
    try:
        return {
            "target": db.session.query(func.round(func.sum(GeneralBranchRankingModel.target))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "tot_qty": db.session.query(func.round(func.sum(GeneralBranchRankingModel.TOTAL_QTY))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "tot_sales": db.session.query(func.round(func.sum(GeneralBranchRankingModel.TOTAL_SALES))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "target_ach_percent": db.session.query(func.round(func.avg(GeneralBranchRankingModel.Target_Ach_percentage))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "avg_qty_sold_per_day": db.session.query(func.round(func.avg(GeneralBranchRankingModel.AVG_VAL_SOLD_PER_DAY))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "avg_qty_ach_target": db.session.query(func.round(func.avg(GeneralBranchRankingModel.AVG_VALUE_ACH_TARGET))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "cm_3month_growth": db.session.query(func.round(func.avg(GeneralBranchRankingModel.CM_3MNTH_GROWTH))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "cm_vs_lm_growth": db.session.query(func.round(func.avg(GeneralBranchRankingModel.CM_LM_GROWTH))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "ach_ratio": db.session.query(func.round(func.avg(GeneralBranchRankingModel.ACH_RATIO))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
            "project_target_ach_percent": db.session.query(func.round(func.avg(GeneralBranchRankingModel.PROJ_TARGET))).filter(
                and_(GeneralBranchRankingModel.section == section, GeneralBranchRankingModel.store_code == store_code)
            ).scalar(),
        }
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_store_section_data(store_code,section)
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)



def get_store_month_year_based_target_monitoring_controller(store_code):
    try:
        
        # data = request.get_json()

        try:
             month = request.args.get('month', type=int)
             year = request.args.get('year', type=int)
        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers", "success": 0 }), 400

        def get_results(section):
            return (
                db.session.query(GeneralBranchRankingModel)
                .filter(
                    and_(
                        GeneralBranchRankingModel.Mmonth == month,
                        GeneralBranchRankingModel.MYear == year,
                        GeneralBranchRankingModel.section == section,
                        GeneralBranchRankingModel.store_code == store_code,
                    )
                )
                .first()
            )

        sections = ["MOBILE", "ACCESSORIES", "WATCH", "INSURANCE"]
        results = {}
        
        for section in sections:
            record = get_results(section)
            if record:
                results[section.lower()] = {
                    "Target": record.target,
                    "Quantity": record.TOTAL_QTY,
                    "AchCategory": record.Target_Ach,
                    "Target_ach_percent": record.Target_Ach_percentage,
                    "Avg_qty_sold_per_day": record.AVG_VAL_SOLD_PER_DAY,
                    "Avg_qty_ach_target": record.AVG_VALUE_ACH_TARGET,
                }
            else:
                results[section.lower()] = {}

        return jsonify(results), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_store_month_year_based_target_monitoring_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)

def get_head_office_stores_sales_controller():
    try:
        try:
             month = request.args.get('month', type=int)
             year = request.args.get('year', type=int)
             section = request.args.get('section')
             last_3mnth_ach = request.args.get('last_3mnth_ach')
             
        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers and section should be string", "success": 0 }), 400
        if last_3mnth_ach:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code).filter(and_(GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)).distinct().all()
            )
        else:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code).filter(GeneralBranchRankingModel.section==section).distinct().all()
            )

        unique_store_code = [store_code[0] for store_code in unique_store_code]
        unique_store_code=list(set(unique_store_code))
        
        result = []
        for store_code in unique_store_code:
            
            if last_3mnth_ach:
            
                count = db.session.query(
                    GeneralBranchRankingModel
                    ).filter(
                        and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)
                        ).count()
            else:
               
                count = db.session.query(
                    GeneralBranchRankingModel
                    ).filter(
                        and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section)
                        ).count()
            
            if count>0:
                if last_3mnth_ach:
                    store_result = (
                        db.session.query(
                            func.sum(GeneralBranchRankingModel.TOTAL_SALES),
                            func.avg(GeneralBranchRankingModel.CM_3MNTH_GROWTH),
                            func.avg(GeneralBranchRankingModel.CM_LM_GROWTH),
                        )
                        .filter(
                            and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)
                            )
                        .first()
                    )
                else:
                    store_result = (
                        db.session.query(
                            func.sum(GeneralBranchRankingModel.TOTAL_SALES),
                            func.avg(GeneralBranchRankingModel.CM_3MNTH_GROWTH),
                            func.avg(GeneralBranchRankingModel.CM_LM_GROWTH),
                        )
                        .filter(
                            and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section)
                            )
                        .first()
                    )
                tot_sales, cm_vs_3m, cm_vs_lm = store_result
                result_dict = {
                    "store_code": store_code,
                    "total_sales": tot_sales,
                    "cm _vs_3mnth_growth": cm_vs_3m,
                    "cm_vs_lm": cm_vs_lm,
                }

                result.append(result_dict)
       
        return jsonify(result)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_head_office_stores_sales_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)

def get_head_office_store_target_achieved_status_controller():
    try:

        try:
             month = request.args.get('month', type=int)
             year = request.args.get('year', type=int)
             section = request.args.get('section')
             last_3mnth_ach = request.args.get('last_3mnth_ach')
             
        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers and section should be string", "success": 0 }), 400
        
        if last_3mnth_ach:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code).filter(and_(GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)).distinct().all()
            )
        else:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code).filter(GeneralBranchRankingModel.section==section).distinct().all()
            )
        unique_store_code = [store_code[0] for store_code in unique_store_code]
        result = []

        for store_code in unique_store_code:

            if last_3mnth_ach:
                count = db.session.query(
                    GeneralBranchRankingModel
                    ).filter(
                        and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)
                        ).count()
            else:
                count = db.session.query(
                    GeneralBranchRankingModel
                    ).filter(
                        and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section)
                        ).count()
            
            if count>0:
                if last_3mnth_ach:
                    store_result = (
                        db.session.query(
                            func.avg(GeneralBranchRankingModel.Target_LM_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.Target_MB_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.Target_2MB_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.ACH_RATIO),
                        )
                        .filter(
                            and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach)
                            )
                        .first()
                    )
                else:
                    store_result = (
                        db.session.query(
                            func.avg(GeneralBranchRankingModel.Target_LM_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.Target_MB_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.Target_2MB_Ach_percentage),
                            func.avg(GeneralBranchRankingModel.ACH_RATIO),
                        )
                        .filter(
                            and_(GeneralBranchRankingModel.store_code == store_code,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.section==section)
                            )
                        .first()
                    )
                
                (
                    target_lm_ach_percent,
                    target_mb_ach_percent,
                    target_2mb_ach_percent,
                    ach_ratio,
                ) = store_result

                result_dict = {
                    "store_code": store_code,
                    "target_lm_ach_percent": target_lm_ach_percent,
                    "target_mb_ach_percent": target_mb_ach_percent,
                    "target_2mb_ach_percent": target_2mb_ach_percent,
                    "ach_ratio": ach_ratio,
                }
                
                result.append(result_dict)
        return result
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_head_office_store_target_achieved_status_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)

#--------------------------------------------------------
def get_head_office_cm_vs_3mnth_avg_growth_controller():
    try:

        try:
             month = request.args.get('month', type=int)
             year = request.args.get('year', type=int)
             section = request.args.get('section')
             last_3mnth_ach = request.args.get('last_3mnth_ach')
             
        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers and section should be string", "success": 0 }), 400
        if last_3mnth_ach:
            
            No_target = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.is_(None),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH<-50.0,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket3 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.between(-50.0,0.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket4 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.between(0.01,50.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket5 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                GeneralBranchRankingModel.CM_3MNTH_GROWTH>50.0,
                GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()
        else:
            No_target = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.is_(None),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                    )
                ).count()

            bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH<-50.0,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                    )
                ).count()

            bucket3 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.between(-50.0,0.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                    )
                ).count()

            bucket4 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_3MNTH_GROWTH.between(0.01,50.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                    )
                ).count()

            bucket5 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                GeneralBranchRankingModel.CM_3MNTH_GROWTH>50.0,
                GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                    )
                ).count()
        result = {"bucket1":No_target,"bucket2":bucket2,"bucket3":bucket3,"bucket4":bucket4,"bucket5":bucket5}

        return result
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_head_office_cm_vs_3mnth_avg_growth_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)
    

def get_head_office_cm_vs_lm_growth_controller():
    try:
        try:
             month = request.args.get('month', type=int)
             year = request.args.get('year', type=int)
             section = request.args.get('section')
             last_3mnth_ach = request.args.get('last_3mnth_ach')
             
        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers and section should be string", "success": 0 }), 400
        if last_3mnth_ach:
            No_target = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_LM_GROWTH.is_(None),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_LM_GROWTH<-50.0,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket3 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_LM_GROWTH.between(-50.0,0.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket4 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                    GeneralBranchRankingModel.CM_LM_GROWTH.between(0.01,50.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()

            bucket5 = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                GeneralBranchRankingModel.CM_LM_GROWTH>50.0,
                GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()
        else:
            No_target = db.session.query(GeneralBranchRankingModel).filter(
            and_(
                GeneralBranchRankingModel.CM_LM_GROWTH.is_(None),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                )
            ).count()

        bucket2 = db.session.query(GeneralBranchRankingModel).filter(
            and_(
                GeneralBranchRankingModel.CM_LM_GROWTH<-50.0,GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                )
            ).count()

        bucket3 = db.session.query(GeneralBranchRankingModel).filter(
            and_(
                GeneralBranchRankingModel.CM_LM_GROWTH.between(-50.0,0.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                )
            ).count()

        bucket4 = db.session.query(GeneralBranchRankingModel).filter(
            and_(
                GeneralBranchRankingModel.CM_LM_GROWTH.between(0.01,50.0),GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                )
            ).count()

        bucket5 = db.session.query(GeneralBranchRankingModel).filter(
            and_(
               GeneralBranchRankingModel.CM_LM_GROWTH>50.0,
               GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year
                )
            ).count()

        result = {"bucket1":No_target,"bucket2":bucket2,"bucket3":bucket3,"bucket4":bucket4,"bucket5":bucket5}

        return result
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_head_office_cm_vs_lm_growth_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)
    

def get_head_office_dendrogram_controller():
    try:
        try:
            month = request.args.get('month', type=int)
            year = request.args.get('year', type=int)
            section = request.args.get('section')
            last_3mnth_ach = request.args.get('last_3mnth_ach')    

        except (KeyError, ValueError) as e:
            return jsonify({"error": "Invalid input, month and year must be provided as integers and section should be string", "success": 0 }), 400
        
        if last_3mnth_ach:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code.distinct()).filter(and_(GeneralBranchRankingModel.section==section, GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year)).all()
            )
        else:
            unique_store_code = (
                db.session.query(GeneralBranchRankingModel.store_code.distinct()).filter(GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year).all()
            )

        unique_store_code = [store_code[0] for store_code in unique_store_code]
        unique_store_code=list(set(unique_store_code))
        total_stores = len(unique_store_code)
        # print(total_stores)  
        # print("hello",last_3mnth_ach)
        if last_3mnth_ach:  
            # print("YEs")
            Not_ach_bucket1_result={}
            Not_ach_bucket2_result={}
            Not_ach_bucket3_result={}
            Not_ach_bucket4_result={}
            Ach_bucket1_result={}
            Ach_bucket2_result={}
            months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
            Not_achieved = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage<100.0,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                )
            ).count()
            # print(Not_achieved)
            achieved = db.session.query(GeneralBranchRankingModel).filter(
                and_(
                GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>=100.0,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                )
            ).count()
            # print(achieved)
            Not_ach_bucket1 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(0.0,50.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()  
            # print(Not_ach_bucket1)

            if Not_ach_bucket1>0:
                    Not_ach_bucket1_result={'name':'0-50% - '+str(Not_ach_bucket1),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(0.0,50.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Not_ach_bucket1_result['children'].append(result)
                    # print(Not_ach_bucket1_result)                              

                                                  
            Not_ach_bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(50.1,75.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count() 
            # print(Not_ach_bucket2) 
               
            if Not_ach_bucket2>0:
                    Not_ach_bucket2_result={'name':'50-75% - '+str(Not_ach_bucket2),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(50.1,75.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Not_ach_bucket2_result['children'].append(result)
                    # print(Not_ach_bucket2_result)                              

            Not_ach_bucket3 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(75.1,89.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()
            # print(Not_ach_bucket3)
            if Not_ach_bucket3>0:
                    Not_ach_bucket3_result={'name':'75-89% - '+str(Not_ach_bucket3),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(75.1,89.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Not_ach_bucket3_result['children'].append(result)
                    # print(Not_ach_bucket3_result)                             

            Not_ach_bucket4 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(89.1,99.9),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count() 
            # print(Not_ach_bucket4)
            if Not_ach_bucket4>0:
                    Not_ach_bucket4_result={'name':'89-100% - '+str(Not_ach_bucket4),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(89.1,99.9),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Not_ach_bucket4_result['children'].append(result)
                    # print(Not_ach_bucket4_result)                              

            achieved_bucket1 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(100.0,120.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()
            # print(achieved_bucket1)

            if achieved_bucket1>0:
                    Ach_bucket1_result={'name':'100-120% - '+str(achieved_bucket1),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(100.0,120.0),GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Ach_bucket1_result['children'].append(result)
                    # print(Ach_bucket1_result)                              

            achieved_bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>120.0,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                    )
                ).count()
            if achieved_bucket2>0:
                    Ach_bucket2_result={'name':'>120% - '+str(achieved_bucket2),'children':[]}
                                                              
                    bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>120,GeneralBranchRankingModel.Last_3mth_ach_count==last_3mnth_ach
                            )
                        ).count() 
                        
                    result = {"name":months[int(last_3mnth_ach)]+str(bucket)}
                    Ach_bucket2_result['children'].append(result)
                    # print(Ach_bucket2_result)  

            if total_stores>0:
                    if Not_achieved>0:
                        children=[]
                        if not Not_ach_bucket1_result=={}:
                            children.append(Not_ach_bucket1_result)
                        if not Not_ach_bucket2_result=={}:
                            children.append(Not_ach_bucket2_result)
                        if not Not_ach_bucket3_result=={}:
                            children.append(Not_ach_bucket3_result)
                        if not Not_ach_bucket4_result=={}:
                            children.append(Not_ach_bucket4_result)
                        not_achieved_result ={'name': 'Not Achieved - '+str(Not_achieved),'children': children}
                        
                    else:
                        not_achieved_result ={}
                        
                    if achieved>0:
                        children=[]
                        if Ach_bucket1_result:
                            children.append(Ach_bucket1_result)
                        if Ach_bucket2_result:
                            children.append(Ach_bucket2_result)
                        achieved_result = {'name':'Achieved - '+str(achieved),'children':children}
                    else:
                        achieved_result=[]

                    final_result = []
                    if not achieved_result=={}:
                        final_result.append(achieved_result)
                    if not not_achieved_result=={}:
                        final_result.append(not_achieved_result)
                    result={'name': 'Total Stores - '+str(total_stores),'children': final_result}                           
                    # print(result)
            else:
                     result={}
      
                                  

           
            
        else:
                Not_ach_bucket1_result={}
                Not_ach_bucket2_result={}
                Not_ach_bucket3_result={}
                Not_ach_bucket4_result={}
                Ach_bucket1_result={}
                Ach_bucket2_result={}
                
                Not_achieved = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage<100.0
                    )
                ).count()
                print("Not achieved :",Not_achieved)
             
                achieved = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>=100.0
                    )
                ).count()
                # print(achieved)
                Not_ach_bucket1 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(0.0,50.0)
                    )
                ).count()  
                # print(Not_ach_bucket1)

                if Not_ach_bucket1>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Not_ach_bucket1_result={'name':'0-50% - '+str(Not_ach_bucket1),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(0.0,50.0),GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Not_ach_bucket1_result['children'].append(result)
                    # print(Not_ach_bucket1_result)                              

                                                  
                Not_ach_bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(50.1,75.0)
                    )
                ).count() 
               
                if Not_ach_bucket2>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Not_ach_bucket2_result={'name':'50-75% - '+str(Not_ach_bucket2),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(50.1,75.0),GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Not_ach_bucket2_result['children'].append(result)
                    # print(Not_ach_bucket2_result)                            

                Not_ach_bucket3 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(75.1,89.0)
                    )
                ).count()
                # print(Not_ach_bucket3)
                if Not_ach_bucket3>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Not_ach_bucket3_result={'name':'50-75% - '+str(Not_ach_bucket3),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(75.1,89.0),GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Not_ach_bucket3_result['children'].append(result)

                    # print(Not_ach_bucket3_result)                              
                

                Not_ach_bucket4 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(89.1,99.9)
                    )
                ).count() 
                # print(Not_ach_bucket4)
                if Not_ach_bucket4>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Not_ach_bucket4_result={'name':'50-75% - '+str(Not_ach_bucket4),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(89.1,99.9),GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Not_ach_bucket4_result['children'].append(result)

                    # print(Not_ach_bucket4_result)                              
                
                achieved_bucket1 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(100.0,120.0)
                    )
                ).count()
                # print(achieved_bucket1)

                if achieved_bucket1>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Ach_bucket1_result={'name':'>=100-120% - '+str(achieved_bucket1),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage.between(100.0,120.0),GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Ach_bucket1_result['children'].append(result)

                    # print(Ach_bucket1_result)                              
                
                achieved_bucket2 = db.session.query(GeneralBranchRankingModel).filter(
                    and_(
                    GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>120.0
                    )
                ).count()
                if achieved_bucket2>0:
                    months=["0M ACH - ","1M ACH - ","2M ACH - ","3M ACH - "]
                    Ach_bucket2_result={'name':'>120% - '+str(achieved_bucket2),'children':[]}
                    
                    for i,mon in zip(range(0,4),months):
                        
                        bucket =  db.session.query(GeneralBranchRankingModel).filter(
                            and_(
                            GeneralBranchRankingModel.section==section,GeneralBranchRankingModel.Mmonth==month,GeneralBranchRankingModel.MYear==year,GeneralBranchRankingModel.Target_Ach_percentage>120.0,GeneralBranchRankingModel.Last_3mth_ach_count==i
                            )
                        ).count() 
                        
                        result = {"name":mon+str(bucket)}
                        Ach_bucket2_result['children'].append(result)

                    # print(Ach_bucket2_result)  
                if total_stores>0:
                    if Not_achieved>0:
                        children=[]
                        if not Not_ach_bucket1_result=={}:
                            children.append(Not_ach_bucket1_result)
                        if not Not_ach_bucket2_result=={}:
                            children.append(Not_ach_bucket2_result)
                        if not Not_ach_bucket3_result=={}:
                            children.append(Not_ach_bucket3_result)
                        if not Not_ach_bucket4_result=={}:
                            children.append(Not_ach_bucket4_result)
                        not_achieved_result ={'name': 'Not Achieved - '+str(Not_achieved),'children': children}
                        
                    else:
                        not_achieved_result ={}
                        
                    if achieved>0:
                        children=[]
                        if Ach_bucket1_result:
                            children.append(Ach_bucket1_result)
                        if Ach_bucket2_result:
                            children.append(Ach_bucket2_result)
                        achieved_result = {'name':'Achieved - '+str(achieved),'children':children}
                    else:
                        achieved_result=[]

                    final_result = []
                    if not achieved_result=={}:
                        final_result.append(achieved_result)
                    if not not_achieved_result=={}:
                        final_result.append(not_achieved_result)
                    result={'name': 'Total Stores - '+str(total_stores),'children': final_result}                          
                    # print(result)
                else:
                        result={}
        
        return jsonify(result)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_head_office_dendrogram_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)
