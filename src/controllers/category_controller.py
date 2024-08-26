from flask import request, jsonify
from src import db
from src.models.general_branch_ranking_model import GeneralBranchRankingModel
from src.models.m_stock_optimization_model import MStockOptimizationModel
from src.models.excess_stock_model import ExcessStockModel


def get_section_controller():
    try:
        result = db.session.query(ExcessStockModel.SECTION.distinct()).all()
        
        result_list = [row[0] for row in result]
        
        return jsonify(result_list)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500

def get_brand_name_controller():
    try:
        result = db.session.query(ExcessStockModel.BRAND.distinct()).all()
        
        result_list = [row[0] for row in result]
        
        return jsonify(result_list)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500

def get_store_name_controller():
    try:
        result = db.session.query(ExcessStockModel.STORE_NAME.distinct()).all()
        
        result_list = [row[0] for row in result]
        
        return jsonify(result_list)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500

def get_item_name_controller():
    try:
        result = db.session.query(ExcessStockModel.MODELNO.distinct()).all()
        
        result_list = [row[0] for row in result]
        
        return jsonify(result_list)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500

def get_store_brand_model_name_controller():
    try:
        result = db.session.query(ExcessStockModel.STATE,ExcessStockModel.CITY,ExcessStockModel.STORE_NAME,ExcessStockModel.STORE_CATEGORY,ExcessStockModel.FRANCH_TYPE,ExcessStockModel.BRAND,ExcessStockModel.MODELNO,ExcessStockModel.STORE_CODE).all()
        res =[]
        for row in result:
            res.append({"STATE":row[0],"CITY":row[1],"STORE":row[2],"STORECATEGORY":row[3],"STORECODE":row[7],"FRANCHTYPE":row[4],"BRAND":row[5],"MODELNUMBER":row[6]})
            
        # print(res)
        # state , city , store, store category, franch type, brand, model
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500
    
def get_state_name_controller():
    try:
        result = db.session.query(MStockOptimizationModel.STATE.distinct()).filter(MStockOptimizationModel.STATE != None).all()
        
        result_list = [row[0] for row in result]
        
        return jsonify(result_list)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500
    
def get_from_store_to_store_brand_model_controller():
    try:
        state = request.args.get("state")
        states = state.split(',')
        result = db.session.query(MStockOptimizationModel.FROM_STORE_CODE,MStockOptimizationModel.TO_STORE_CODE,MStockOptimizationModel.BRAND,MStockOptimizationModel.MODELNO).filter(MStockOptimizationModel.STATE.in_(states)).all()
        res=[]
        for row in result:
            res.append({"FROM_STORE_CODE":row[0],"TO_STORE_CODE":row[1],"BRAND":row[2],"MODELNO":row[3]})
            
        
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": 0, "error": str(e)}), 500