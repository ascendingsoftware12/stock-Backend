from operator import and_
from flask import request, jsonify
from src import db
from src.models.m_stock_optimization_model import MStockOptimizationModel
from datetime import date


def get_approve_transfer_controller():
    try:
        from_store_code = request.args.get("fromstorecode")
        to_store_code = request.args.get("tostorecode")
        model_number = request.args.get("modelnumber")
        brand = request.args.get("brand")
        transfer_type = request.args.get("transferType")
        state = request.args.get("state")

        query = MStockOptimizationModel.query.with_entities(
            MStockOptimizationModel.FROM_STORE_CODE,
            MStockOptimizationModel.FROM_STORE_NAME,
            MStockOptimizationModel.STATE,
            MStockOptimizationModel.TO_STORE_NAME,
            MStockOptimizationModel.TO_STORE_CODE,
            MStockOptimizationModel.MODELNO,
            MStockOptimizationModel.BRAND,
            MStockOptimizationModel.approved_qty,
            MStockOptimizationModel.SUPPLIED_QTY,
            MStockOptimizationModel.REQUESTED_QTY,
            MStockOptimizationModel.ID_NO,
            MStockOptimizationModel.TRANS_FLAG,
            MStockOptimizationModel.t_approved_flag,
            MStockOptimizationModel.SUPPLIED_QTY,
        ).filter(
                MStockOptimizationModel.FROM_STORE_CODE.isnot(None)
        ).filter(
            MStockOptimizationModel.TO_STORE_NAME.isnot(None)
        ).filter(
            MStockOptimizationModel.SUPPLIED_QTY > 0
        )

        if from_store_code:
            query = query.filter(
                MStockOptimizationModel.FROM_STORE_CODE == from_store_code
            )
        if to_store_code:
            query = query.filter(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
        if model_number:
            query = query.filter(MStockOptimizationModel.MODELNO == model_number)
        if brand:
            query = query.filter(MStockOptimizationModel.BRAND == brand)
        if transfer_type:
            query = query.filter(MStockOptimizationModel.TRANS_FLAG == transfer_type)
        if state:
            states = state.split(",")
            query = query.filter(MStockOptimizationModel.STATE.in_(states))

        records = query.order_by(MStockOptimizationModel.t_approved_flag.desc()).all()

        result = []
        for row in records:
            group_data = {
                "FROM_STORE_NAME": row.FROM_STORE_NAME,
                "FROM_STORE_CODE": row.FROM_STORE_CODE,
                "STATE": row.STATE,
                "TO_STORE_NAME": row.TO_STORE_NAME,
                "TO_STORE_CODE": row.TO_STORE_CODE,
                "MODEL_NUMBER": row.MODELNO,
                "BRAND": row.BRAND,
                "REQUESTED_QUANTITY": row.REQUESTED_QTY,
                "TRANSFER_QUANTITY": row.SUPPLIED_QTY,
                "TRANSPORT_TYPE": row.TRANS_FLAG,
                "ID": row.ID_NO,
                "Approval_flag": row.t_approved_flag,
            }

            if row.t_approved_flag == "TRUE":
                group_data["APPROVED_QUANTITY"] = row.approved_qty
            else:
                group_data["APPROVED_QUANTITY"] = row.SUPPLIED_QTY
            result.append(group_data)

        return jsonify(result)
    except Exception as e:
        return jsonify({"success": 0, "err": str(e)}), 500


def get_approve_transfer_controller1():
    try:
        records = MStockOptimizationModel.query.with_entities(
            MStockOptimizationModel.FROM_STORE_CODE,
            MStockOptimizationModel.FROM_STORE_NAME,
            MStockOptimizationModel.TO_STORE_NAME,
            MStockOptimizationModel.TO_STORE_CODE,
            MStockOptimizationModel.MODELNO,
            MStockOptimizationModel.BRAND,
            MStockOptimizationModel.approved_qty,
            MStockOptimizationModel.SUPPLIED_QTY,
            MStockOptimizationModel.REQUESTED_QTY,
            MStockOptimizationModel.ID_NO,
            MStockOptimizationModel.TRANS_FLAG,
            MStockOptimizationModel.t_approved_flag,
        ).all()
        result = []
        for row in records:
            group_data = {
                "FROM_STORE_NAME": row.FROM_STORE_NAME,
                "FROM_STORE_CODE": row.FROM_STORE_CODE,
                "TO_STORE_NAME": row.TO_STORE_NAME,
                "TO_STORE_CODE": row.TO_STORE_CODE,
                "MODEL_NUMBER": row.MODELNO,
                "BRAND": row.BRAND,
                "APPROVED_QUANTITY": row.approved_qty,
                "TRANSFER_QUANTITY": row.SUPPLIED_QTY,
                "REQUESTED_QUANTITY": row.REQUESTED_QTY,
                "TRANSPORT_TYPE": row.TRANS_FLAG,
                "ID": row.ID_NO,
                "Approval_flag": row.t_approved_flag,
            }
            result.append(group_data)

        return result
    except Exception as e:
        return jsonify({"success": 0, "err": str(e)}), 500


def update_approve_transfer_controller():
    try:
        data = request.get_json()

        for row in data:
            id = row
            approved_qty = data[str(row)]
            record = MStockOptimizationModel.query.filter_by(ID_NO=id).first()
            record.approved_qty = approved_qty
            record.t_approved_flag = "TRUE"
            record.t_approved_by = "HO"
            record.t_approved_date = date.today()

        db.session.commit()
        return jsonify({"success": 1, "message": "Approved Quantity updated"})
    except Exception as e:
        return jsonify({"success": 0, "err": str(e)}), 500
