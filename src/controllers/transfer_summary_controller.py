from flask import request, jsonify
from src import db
from src.models.m_stock_optimization_model import MStockOptimizationModel


def get_tranfer_summary_controller(storecode):
    try:
        to_store_code = request.args.get("tostorecode")
        model_number = request.args.get("modelnumber")
        brand = request.args.get("brand")

        query = MStockOptimizationModel.query.with_entities(
            MStockOptimizationModel.ID_NO,
            MStockOptimizationModel.TO_STORE_CODE,
            MStockOptimizationModel.TO_STORE_NAME,
            MStockOptimizationModel.MODELNO,
            MStockOptimizationModel.BRAND,
            MStockOptimizationModel.SUPPLY_QTY,
            MStockOptimizationModel.approved_qty,
            MStockOptimizationModel.t_couriered_qty,
            MStockOptimizationModel.d_couriered_date,
            MStockOptimizationModel.t_couriered_flag,
        ).filter(MStockOptimizationModel.FROM_STORE_CODE == storecode, MStockOptimizationModel.t_approved_flag == "TRUE")
        if to_store_code:
            query = query.filter(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
        if model_number:
            query = query.filter(MStockOptimizationModel.MODELNO == model_number)
        if brand:
            query = query.filter(MStockOptimizationModel.BRAND == brand)

        tranfer_summary_results = query.all()

        data = [
            {
                "ID_NO": result.ID_NO,
                "TO_STORE_CODE": result.TO_STORE_CODE,
                "TO_STORE_NAME": result.TO_STORE_NAME,
                "MODELNO": result.MODELNO,
                "BRAND": result.BRAND,
                "SUPPLY_QTY": result.SUPPLY_QTY,
                "approved_qty": result.approved_qty,
                "t_couriered_qty": result.t_couriered_qty,
                "d_couriered_date": result.d_couriered_date,
                "t_couriered_flag": result.t_couriered_flag,
            }
            for result in tranfer_summary_results
        ]

        return jsonify(data), 201
    except Exception as e:
        return jsonify({"error": str(e), "success": 0}), 500


def update_tranfer_summary_controller():
    data = request.get_json()

    if not data or not isinstance(data, list):
        return jsonify({"error": "Invalid data format", "success": 0}), 400

    for item in data:
        record_id = item.get("id")
        couriered_quantity = item.get("courieredQuantity")
        couriered_date = item.get("courieredDate")

        if record_id is None or couriered_quantity is None or couriered_date is None:
            return jsonify({"error": "Missing fields in data", "success": 0}), 400

        record = MStockOptimizationModel.query.get(record_id)
        if record:
            record.t_couriered_qty = couriered_quantity
            record.d_couriered_date = couriered_date
            record.t_couriered_flag = "TRUE"

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e), "success": 0}), 500

    return jsonify({"message": "Records updated successfully", "success": 1}), 200
