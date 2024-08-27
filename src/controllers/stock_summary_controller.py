from flask import jsonify, request
from sqlalchemy import func, case, or_
from sqlalchemy.orm import aliased
from src.models.m_stock_optimization_model import MStockOptimizationModel
from src import db

def get_headoffice_stock_summary_list_controller():
    try:
        from_store_code = request.args.get("fromstorecode")
        state = request.args.get("state")
        Report = aliased(MStockOptimizationModel)

        query = (
            db.session.query(
                Report.FROM_STORE_CODE,
                Report.STATE,
                func.sum(
                    case(
                        (
                            or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                            func.coalesce(Report.SUPPLY_QTY, 0)
                            - func.coalesce(Report.approved_qty, 0),
                        ),
                        else_=0,
                    )
                ).label("excess_stock_qty"),
                func.sum(
                    case(
                        (
                            Report.EOL_FLAG == "Y",
                            func.coalesce(Report.SUPPLY_QTY, 0)
                            - func.coalesce(Report.approved_qty, 0),
                        ),
                        else_=0,
                    )
                ).label("dead_stock_qty"),
                func.sum(
                    case((Report.EOL_FLAG == "Y", Report.COST_PRICE), else_=0)
                ).label("dead_stock_price"),
                (
                    func.sum(
                        case(
                            (
                                or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                                func.coalesce(Report.SUPPLY_QTY, 0)
                                - func.coalesce(Report.approved_qty, 0),
                            ),
                            else_=0,
                        )
                    )
                    + func.sum(
                        case(
                            (
                                Report.EOL_FLAG == "Y",
                                func.coalesce(Report.SUPPLY_QTY, 0)
                                - func.coalesce(Report.approved_qty, 0),
                            ),
                            else_=0,
                        )
                    )
                ).label("total_qty"),
                func.sum(
                    case(
                        (
                            or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                            Report.COST_PRICE,
                        ),
                        else_=0,
                    )
                ).label("excess_stock_price"),
                (
                    func.sum(
                        case(
                            (
                                or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                                Report.COST_PRICE,
                            ),
                            else_=0,
                        )
                    )
                    + func.sum(
                        case((Report.EOL_FLAG == "Y", Report.COST_PRICE), else_=0)
                    )
                ).label("total_price"),
            )
            .filter(Report.FROM_STORE_CODE != None)
            .group_by(Report.FROM_STORE_CODE)
        )

        if from_store_code:
            query = query.filter(Report.FROM_STORE_CODE == from_store_code)
        if state:
            states = state.split(',')
            query = query.filter(Report.STATE.in_(states))

        results = query.all()

        summary_data = []
        for result in results:
            summary_data.append(
                {
                    "FROM_STORE_CODE": result.FROM_STORE_CODE,
                    "STATE": result.STATE,
                    "excess_stock_qty": result.excess_stock_qty,
                    "dead_stock_qty": result.dead_stock_qty,
                    "total_qty": result.total_qty,
                    "excess_stock_price": result.excess_stock_price,
                    "dead_stock_price": result.dead_stock_price,
                    "total_price": result.total_price,
                }
            )

        return jsonify(summary_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_store_stock_summary_list_controller(store_code):
    try:
        model_number = request.args.get("modelnumber")
        item_name=request.args.get("itemname")
        brand=request.args.get("brand")
        eol_flag = request.args.get("eolflag")

        query = ( 
            db.session.query(
                MStockOptimizationModel,
                MStockOptimizationModel.FROM_STORE_CODE,
                MStockOptimizationModel.MODELNO,
                MStockOptimizationModel.BRAND,
                MStockOptimizationModel.ITEM_NAME,
                MStockOptimizationModel.EOL_FLAG,
                case(
                    (
                        or_(
                            MStockOptimizationModel.EOL_FLAG == "N",
                            MStockOptimizationModel.EOL_FLAG == None,
                        ),
                        func.coalesce(MStockOptimizationModel.SUPPLY_QTY, 0)
                        - func.coalesce(MStockOptimizationModel.approved_qty, 0),
                    ),
                    else_=0,
                ).label("excess_stock_qty"),
                case(
                    (
                        or_(
                            MStockOptimizationModel.EOL_FLAG == "N",
                            MStockOptimizationModel.EOL_FLAG == None,
                        ),
                        MStockOptimizationModel.COST_PRICE,
                    ),
                    else_=0,
                ).label("excess_stock_price"),
                case(
                    (
                        MStockOptimizationModel.EOL_FLAG == "Y",
                        func.ifnull(MStockOptimizationModel.SUPPLY_QTY, 0)
                        - func.ifnull(MStockOptimizationModel.approved_qty, 0),
                    ),
                    else_=0,
                ).label("dead_stock_qty"),
                case(
                    (
                        MStockOptimizationModel.EOL_FLAG == "Y",
                        MStockOptimizationModel.COST_PRICE,
                    ),
                    else_=0,
                ).label("dead_stock_price"),
            )
            .filter(MStockOptimizationModel.FROM_STORE_CODE == store_code)
        )

        if model_number:
            query = query.filter(MStockOptimizationModel.MODELNO == model_number)
        if item_name:
            query = query.filter(MStockOptimizationModel.ITEM_NAME == item_name)
        if brand:
            query = query.filter(MStockOptimizationModel.BRAND == brand)
        if eol_flag:
            query = query.filter(MStockOptimizationModel.EOL_FLAG == eol_flag)

        results = query.all()

        summary_data = []
        for result in results:
            summary_data.append(
                {
                    "FROM_STORE_CODE": result.FROM_STORE_CODE,
                    "MODELNO": result.MODELNO,
                    "BRAND": result.BRAND,
                    "ITEM_NAME": result.ITEM_NAME,
                    "EOL_FLAG": result.EOL_FLAG,
                    "excess_stock_qty": result.excess_stock_qty,
                    "dead_stock_qty": result.dead_stock_qty,
                    # "total_qty": result.excess_stock_qty + result.dead_stock_qty,
                    "excess_stock_price": result.excess_stock_price,
                    "dead_stock_price": result.dead_stock_price,
                    # "total_price": result.excess_stock_price + result.dead_stock_price,
                }
            )

        return jsonify(summary_data), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500






# old 

def get_store_stock_summary_list_controller1(store_code):
    try:

        model_number = request.args.get("modelnumber")
        eol_flag = request.args.get("eolflag")

        Report = aliased(MStockOptimizationModel)

        query = (
            db.session.query(
                Report.FROM_STORE_CODE,
                Report.MODELNO,
                Report.EOL_FLAG,
                func.sum(
                    case(
                        (
                            or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                            func.coalesce(Report.SUPPLY_QTY, 0)
                            - func.coalesce(Report.approved_qty, 0),
                        ),
                        else_=0,
                    )
                ).label("excess_stock_qty"),
                func.sum(
                    case(
                        (
                            Report.EOL_FLAG == "Y",
                            func.coalesce(Report.SUPPLY_QTY, 0)
                            - func.coalesce(Report.approved_qty, 0),
                        ),
                        else_=0,
                    )
                ).label("dead_stock_qty"),
                func.sum(
                    case((Report.EOL_FLAG == "Y", Report.COST_PRICE), else_=0)
                ).label("dead_stock_price"),
                (
                    func.sum(
                        case(
                            (
                                or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                                func.coalesce(Report.SUPPLY_QTY, 0)
                                - func.coalesce(Report.approved_qty, 0),
                            ),
                            else_=0,
                        )
                    )
                    + func.sum(
                        case(
                            (
                                Report.EOL_FLAG == "Y",
                                func.coalesce(Report.SUPPLY_QTY, 0)
                                - func.coalesce(Report.approved_qty, 0),
                            ),
                            else_=0,
                        )
                    )
                ).label("total_qty"),
                func.sum(
                    case(
                        (
                            or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                            Report.COST_PRICE,
                        ),
                        else_=0,
                    )
                ).label("excess_stock_price"),
                (
                    func.sum(
                        case(
                            (
                                or_(Report.EOL_FLAG == "N", Report.EOL_FLAG == None),
                                Report.COST_PRICE,
                            ),
                            else_=0,
                        )
                    )
                    + func.sum(
                        case((Report.EOL_FLAG == "Y", Report.COST_PRICE), else_=0)
                    )
                ).label("total_price"),
            )
            .filter(Report.FROM_STORE_CODE == store_code)
            .group_by(Report.FROM_STORE_CODE)
        )

        if model_number:
            query = query.filter(Report.MODELNO == model_number)
        if eol_flag:
            query = query.filter(Report.EOL_FLAG == eol_flag)

        results = query.all()

        summary_data = []
        for result in results:
            summary_data.append(
                {
                    "FROM_STORE_CODE": result.FROM_STORE_CODE,
                    "MODELNO": result.MODELNO,
                    "EOL_FLAG": result.EOL_FLAG,
                    "excess_stock_qty": result.excess_stock_qty,
                    "dead_stock_qty": result.dead_stock_qty,
                    "total_qty": result.total_qty,
                    "excess_stock_price": result.excess_stock_price,
                    "dead_stock_price": result.dead_stock_price,
                    "total_price": result.total_price,
                }
            )

        return jsonify(summary_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
