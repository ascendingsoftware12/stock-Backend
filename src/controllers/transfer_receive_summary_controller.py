from flask import request, jsonify
from src import db
from src.models.m_stock_optimization_model import MStockOptimizationModel


def get_tranfer_receive_summary_controller(store_code):
    try:
        to_store_code = request.args.get('tostorecode')
        model_number = request.args.get('modelnumber')
        brand = request.args.get('brand')
        item_name = request.args.get('itemname')
        query = MStockOptimizationModel.query.with_entities(
            MStockOptimizationModel.ID_NO,
            MStockOptimizationModel.FROM_STORE_CODE,
            MStockOptimizationModel.FROM_STORE_NAME,
            MStockOptimizationModel.MODELNO,
            MStockOptimizationModel.BRAND,
            MStockOptimizationModel.ITEM_NAME,
            MStockOptimizationModel.SUPPLY_QTY,
            MStockOptimizationModel.approved_qty,
            MStockOptimizationModel.t_couriered_qty,
            MStockOptimizationModel.t_received_qty,
            MStockOptimizationModel.d_received_date,
            MStockOptimizationModel.t_received_flag,
        ).filter(MStockOptimizationModel.TO_STORE_CODE == store_code, MStockOptimizationModel.t_couriered_flag == "TRUE")
        if to_store_code:
            query = query.filter(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
        if model_number:
            query = query.filter(MStockOptimizationModel.MODELNO == model_number)
        if brand:
            query = query.filter(MStockOptimizationModel.BRAND == brand)
        if item_name:
            query = query.filter(MStockOptimizationModel.ITEM_NAME == item_name)
        tranfer_receive_summary_results= query.all()

        data = [
            {
                "ID_NO": result.ID_NO,
                "FROM_STORE_CODE": result.FROM_STORE_CODE,
                "FROM_STORE_NAME": result.FROM_STORE_NAME,
                "MODELNO": result.MODELNO,
                "BRAND": result.BRAND,
                "ITEM_NAME": result.ITEM_NAME,
                "SUPPLY_QTY": result.SUPPLY_QTY,
                "approved_qty": result.approved_qty,
                "t_couriered_qty": result.t_couriered_qty,
                "t_received_qty": result.t_received_qty,
                "d_received_date": result.d_received_date,
                "t_received_flag": result.t_received_flag,
            }
            for result in tranfer_receive_summary_results
        ]

        return jsonify(data), 201
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_tranfer_receive_summary_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def update_tranfer_receive_summary_controller():
    data = request.get_json()

    if not data or not isinstance(data, list):
        return jsonify({"error": "Invalid data format", "success": 0}), 400

    for item in data:
        record_id = item.get("id")
        received_quantity = item.get("receivedQuantity")
        received_date = item.get("receivedDate")

        if record_id is None or received_quantity is None or received_date is None:
            return jsonify({"error": "Missing fields in data", "success": 0}), 400

        record = MStockOptimizationModel.query.get(record_id)
        if record:
            record.t_received_qty = received_quantity
            record.d_received_date = received_date
            record.t_received_flag = "TRUE"

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return update_tranfer_receive_summary_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)

    return jsonify({"message": "Records updated successfully", "success": 1}), 200
