from operator import and_, is_, or_
from flask import request, jsonify
from src import db
from datetime import datetime
from src.models.m_stock_optimization_model import MStockOptimizationModel
from sqlalchemy import func, case


def get_transfer_report_controller():
    try:
        from_store_code = request.args.get("fromstorecode")
        to_store_code = request.args.get("tostorecode")
        model_number = request.args.get("modelnumber")
        brand = request.args.get("brand")
        status = request.args.get("status")
        state = request.args.get("state")
        item_name = request.args.get('itemname')

        query = MStockOptimizationModel.query.with_entities(
            MStockOptimizationModel.FROM_STORE_CODE,
            MStockOptimizationModel.FROM_STORE_NAME,
            MStockOptimizationModel.STATE,
            MStockOptimizationModel.TO_STORE_NAME,
            MStockOptimizationModel.TO_STORE_CODE,
            MStockOptimizationModel.MODELNO,
            MStockOptimizationModel.BRAND,
            MStockOptimizationModel.approved_qty,
            MStockOptimizationModel.t_couriered_qty,
            MStockOptimizationModel.REQUESTED_QTY,
            MStockOptimizationModel.ID_NO,
            MStockOptimizationModel.SUPPLIED_QTY,
            MStockOptimizationModel.t_received_qty,
            MStockOptimizationModel.t_approved_date,
            MStockOptimizationModel.d_received_date,
            MStockOptimizationModel.d_couriered_date,
            MStockOptimizationModel.ITEM_NAME
        )

        if from_store_code:
            query = query.filter(
                MStockOptimizationModel.FROM_STORE_CODE == from_store_code,
            )
        if to_store_code:
            query = query.filter(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
        if model_number:
            query = query.filter(MStockOptimizationModel.MODELNO == model_number)
        if brand:
            query = query.filter(MStockOptimizationModel.BRAND == brand)
        if item_name:
            query = query.filter(MStockOptimizationModel.ITEM_NAME == item_name)
        if state:
            states = state.split(',')
            query = query.filter(MStockOptimizationModel.STATE.in_(states))

        if status == "All":
            pass
        elif status == "ANC":
            query = query.filter(
                and_(
                        MStockOptimizationModel.t_approved_flag == "TRUE",
                        and_(
                            MStockOptimizationModel.t_couriered_flag.is_(None),
                            MStockOptimizationModel.t_received_flag==None     
                        )
                    )
                
            )
                

        elif status == "CNR":
            query = query.filter(
                 and_(
                        MStockOptimizationModel.t_approved_flag == "TRUE",
                        and_(
                            MStockOptimizationModel.t_couriered_flag == "TRUE",
                            MStockOptimizationModel.t_received_flag==None     
                        )
                    )
            )

        records = query.filter(and_(MStockOptimizationModel.SUPPLIED_QTY > 0,MStockOptimizationModel.approved_qty>0)).all()

        result = []
        for row in records:
            group_data = {
                "FROM_STORE_NAME": row.FROM_STORE_NAME,
                "FROM_STORE_CODE": row.FROM_STORE_CODE,
                "STATE": row.STATE,
                "TO_STORE_NAME": row.TO_STORE_NAME,
                "TO_STORE_CODE": row.TO_STORE_CODE,
                "MODEL_NUMBER": row.MODELNO,
                "ITEM_NAME":row.ITEM_NAME,
                "BRAND": row.BRAND,
                "RECOMMENDED_QUANTITY": row.SUPPLIED_QTY,
                "APPROVED_QUANTITY": row.approved_qty,
               
                "REQUESTED_QUANTITY": row.REQUESTED_QTY,
                "TRANSFER_QUANTITY": row.t_couriered_qty,
                "TRANSFERRED_DATE": row.d_couriered_date,
                "RECEIVED_QUANTITY": row.t_received_qty,
                "RECEIVED_DATE": row.d_received_date,
                "ID": row.ID_NO,
            }
            if row.t_approved_date is not None:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.t_approved_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["APPROVED_DATE"]= str(formatted_date)
               
            else:
                group_data["APPROVED_DATE"]= ""
            if row.d_couriered_date is not None:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.d_couriered_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["TRANSFERRED_DATE"]= str(formatted_date)
        
            if row.d_received_date is not None:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.d_received_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["RECEIVED_DATE"]= str(formatted_date)

            result.append(group_data)

        records = query.filter(and_(MStockOptimizationModel.SUPPLIED_QTY > 0,MStockOptimizationModel.approved_qty.is_(None))).all()

        
        for row in records:
            group_data = {
                "FROM_STORE_NAME": row.FROM_STORE_NAME,
                "FROM_STORE_CODE": row.FROM_STORE_CODE,
                "STATE": row.STATE,
                "TO_STORE_NAME": row.TO_STORE_NAME,
                "TO_STORE_CODE": row.TO_STORE_CODE,
                "MODEL_NUMBER": row.MODELNO,
                "ITEM_NAME":row.ITEM_NAME,
                "BRAND": row.BRAND,
                "RECOMMENDED_QUANTITY": row.SUPPLIED_QTY,
                "APPROVED_QUANTITY": row.approved_qty,               
                "REQUESTED_QUANTITY": row.REQUESTED_QTY,
                "TRANSFER_QUANTITY": row.t_couriered_qty,
                "RECEIVED_QUANTITY": row.t_received_qty,
                "ID": row.ID_NO,
            }
            if row.t_approved_date:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.t_approved_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["APPROVED_DATE"]= str(formatted_date)
                
            else:
                group_data["APPROVED_DATE"]= ""

            if row.d_couriered_date:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.d_couriered_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["TRANSFERRED_DATE"]= str(formatted_date)
        
            if row.d_received_date:
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(str(row.d_received_date), '%Y-%m-%d')

                # Format the datetime object into DD-MM-YYYY format
                formatted_date = date_obj.strftime('%d-%m-%Y')
                group_data["RECEIVED_DATE"]= str(formatted_date)

            result.append(group_data)
    
        return jsonify(result)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_transfer_report_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_overall_transfer_report_count_controller():
    from_store_code = request.args.get("fromstorecode")
    to_store_code = request.args.get("tostorecode")
    model_number = request.args.get("modelnumber")
    brand = request.args.get("brand")
    status = request.args.get("status")
    state = request.args.get("state")
    item_name = request.args.get('itemname')

    latest_opt_date_subquery = (
        db.session.query(
            func.max(MStockOptimizationModel.OPT_DATE).label("latest_opt_date")
        )
        .filter(MStockOptimizationModel.SUPPLY_QTY > 0)
        .subquery()
    )

    filters = [MStockOptimizationModel.OPT_DATE == latest_opt_date_subquery.c.latest_opt_date]

    if from_store_code:
        filters.append(MStockOptimizationModel.FROM_STORE_CODE == from_store_code)
    if to_store_code:
        filters.append(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
    if model_number:
        filters.append(MStockOptimizationModel.MODELNO == model_number)
    if item_name:
            query = query.filter(MStockOptimizationModel.ITEM_NAME == item_name)
    if brand:
        filters.append(MStockOptimizationModel.BRAND == brand)
    if state:
        filters.append(MStockOptimizationModel.STATE == state)
    if status == "All":
            pass
    
    elif status == "ANC":
            filters.append(
                
                    and_(
                        MStockOptimizationModel.t_approved_flag == "TRUE",
                        and_(
                            MStockOptimizationModel.t_couriered_flag.is_(None),
                            MStockOptimizationModel.t_received_flag==None     
                        )
                    )
                
            )

    elif status == "CNR":
        filters.append(
            and_(
                        MStockOptimizationModel.t_approved_flag == "TRUE",
                        and_(
                            MStockOptimizationModel.t_couriered_flag == "TRUE",
                            MStockOptimizationModel.t_received_flag==None     
                        )
                    )
        )

    stats_query = (
        db.session.query(
            func.count(case((MStockOptimizationModel.SUPPLIED_QTY > 0, 1))).label(
                "TotalTransfers"
            ),
            func.count(
                case((MStockOptimizationModel.t_approved_flag == "true", 1))
            ).label("ApprovedTransfers"),
            func.count(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("TransfersbyStore"),
            func.count(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("TransfersReceived"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.SUPPLIED_QTY > 0,
                            MStockOptimizationModel.OPT_DATE
                            == latest_opt_date_subquery.c.latest_opt_date,
                        ),
                        MStockOptimizationModel.SUPPLIED_QTY,
                    ),
                    else_=0,
                )
            ).label("TotalRecQuantity"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.SUPPLY_QTY,
                    ),
                    else_=0,
                )
            ).label("RecommendedQuantity"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.approved_qty,
                    ),
                    else_=0,
                )
            ).label("ApprovedQuantityByHO"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        MStockOptimizationModel.t_couriered_qty,
                    ),
                    else_=0,
                )
            ).label("TransferedQuantity"),
            func.sum(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        MStockOptimizationModel.t_received_qty,
                    ),
                    else_=0,
                )
            ).label("ReceivedQuantity"),
        )
        .select_from(MStockOptimizationModel)
        .filter(
            # and_(
                MStockOptimizationModel.OPT_DATE
                == latest_opt_date_subquery.c.latest_opt_date
            # )
        )
        .filter(*filters)
        .limit(1)
        .one()
    )

    response = {
        "Total_Transfers": stats_query.TotalTransfers,
        "Total_Rec_Quantity": stats_query.TotalRecQuantity,
        "Approved_Transfers": stats_query.ApprovedTransfers,
        "Recommended_Quantity": stats_query.RecommendedQuantity,
        "Approved_Quantity_By_HO": stats_query.ApprovedQuantityByHO,
        "Transfers_by_Store": stats_query.TransfersbyStore,
        "Transfered_Quantity": stats_query.TransferedQuantity,
        "Transfers_Received": stats_query.TransfersReceived,
        "Received_Quantity": stats_query.ReceivedQuantity,
    }

    return jsonify(response)



def get_overall_transfer_report_count_controller2():
    from_store_code = request.args.get("fromstorecode")
    to_store_code = request.args.get("tostorecode")
    model_number = request.args.get("modelnumber")
    brand = request.args.get("brand")
    status = request.args.get("status")
    
    latest_opt_date_subquery = (
        db.session.query(
            func.max(MStockOptimizationModel.OPT_DATE).label("latest_opt_date")
        )
        .filter(MStockOptimizationModel.SUPPLY_QTY > 0)
        .subquery()
    )

    filters = [MStockOptimizationModel.OPT_DATE == latest_opt_date_subquery.c.latest_opt_date]

    if from_store_code:
        filters.append(MStockOptimizationModel.FROM_STORE_CODE == from_store_code)
    if to_store_code:
        filters.append(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
    if model_number:
        filters.append(MStockOptimizationModel.MODELNO == model_number)
    if brand:
        filters.append(MStockOptimizationModel.BRAND == brand)
    if status == "All":
            pass
    elif status == "ANC":
        filters.append(
            or_(
                and_(
                    MStockOptimizationModel.t_approved_date != None,
                    MStockOptimizationModel.d_couriered_date.is_(None),
                ),
                and_(
                    MStockOptimizationModel.t_approved_date != "",
                    MStockOptimizationModel.d_couriered_date == "",
                ),
            )
        )

    elif status == "CNR":
        filters.append(
            or_(
                MStockOptimizationModel.d_received_date == None,
                MStockOptimizationModel.d_received_date == "",
            )
        )

    stats_query = (
        db.session.query(
            func.count(case((MStockOptimizationModel.SUPPLIED_QTY > 0, 1))).label(
                "TotalTransfers"
            ),
            func.count(
                case((MStockOptimizationModel.t_approved_flag == "true", 1))
            ).label("ApprovedTransfers"),
            func.count(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("TransfersbyStore"),
            func.count(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("TransfersReceived"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.SUPPLIED_QTY > 0,
                            MStockOptimizationModel.OPT_DATE
                            == latest_opt_date_subquery.c.latest_opt_date,
                        ),
                        MStockOptimizationModel.SUPPLIED_QTY,
                    ),
                    else_=0,
                )
            ).label("TotalRecQuantity"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.SUPPLY_QTY,
                    ),
                    else_=0,
                )
            ).label("RecommendedQuantity"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.approved_qty,
                    ),
                    else_=0,
                )
            ).label("ApprovedQuantityByHO"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        MStockOptimizationModel.t_couriered_qty,
                    ),
                    else_=0,
                )
            ).label("TransferedQuantity"),
            func.sum(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        MStockOptimizationModel.t_received_qty,
                    ),
                    else_=0,
                )
            ).label("ReceivedQuantity"),
        )
        .select_from(MStockOptimizationModel)
        .filter(
            # and_(
                MStockOptimizationModel.OPT_DATE
                == latest_opt_date_subquery.c.latest_opt_date
            # )
        )
        .filter(*filters)
        .limit(1)
        .one()
    )

    response = {
        "Total_Transfers": stats_query.TotalTransfers,
        "Total_Rec_Quantity": stats_query.TotalRecQuantity,
        "Approved_Transfers": stats_query.ApprovedTransfers,
        "Recommended_Quantity": stats_query.RecommendedQuantity,
        "Approved_Quantity_By_HO": stats_query.ApprovedQuantityByHO,
        "Transfers_by_Store": stats_query.TransfersbyStore,
        "Transfered_Quantity": stats_query.TransferedQuantity,
        "Transfers_Received": stats_query.TransfersReceived,
        "Received_Quantity": stats_query.ReceivedQuantity,
    }

    return jsonify(response)


def get_overall_transfer_report_count_controller1():
    from_store_code = request.args.get("fromstorecode")
    to_store_code = request.args.get("tostorecode")
    model_number = request.args.get("modelnumber")
    brand = request.args.get("brand")
    status = request.args.get("status")

    latest_opt_date_subquery = (
        db.session.query(
            func.max(MStockOptimizationModel.OPT_DATE).label("latest_opt_date")
        )
        .filter(MStockOptimizationModel.SUPPLY_QTY > 0)
        .subquery()
    )

    filters = [MStockOptimizationModel.OPT_DATE == latest_opt_date_subquery.c.latest_opt_date]

    if from_store_code:
        filters.append(MStockOptimizationModel.from_store_code == from_store_code)
    if to_store_code:
        filters.append(MStockOptimizationModel.to_store_code == to_store_code)
    if model_number:
        filters.append(MStockOptimizationModel.model_number == model_number)
    if brand:
        filters.append(MStockOptimizationModel.brand == brand)
    if status:
        filters.append(MStockOptimizationModel.status == status)

    stats_query = (
        db.session.query(
            func.count(case((MStockOptimizationModel.SUPPLY_QTY > 0, 1))).label("totalTransfers"),
            func.count(case((MStockOptimizationModel.t_approved_flag == "true", 1))).label("approvedTransfers"),
            func.count(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("noOfTransfers"),
            func.count(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        1,
                    )
                )
            ).label("noOfReceived"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.SUPPLY_QTY > 0,
                            MStockOptimizationModel.OPT_DATE == latest_opt_date_subquery.c.latest_opt_date,
                        ),
                        MStockOptimizationModel.SUPPLY_QTY,
                    ),
                    else_=0,
                )
            ).label("suggestedTransferQuantity"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.SUPPLY_QTY,
                    ),
                    else_=0,
                )
            ).label("approvedOptmizedQty"),
            func.sum(
                case(
                    (
                        MStockOptimizationModel.t_approved_flag == "true",
                        MStockOptimizationModel.approved_qty,
                    ),
                    else_=0,
                )
            ).label("approvedQty"),
            func.sum(
                case(
                    (
                        and_(
                            MStockOptimizationModel.t_approved_flag == "true",
                            MStockOptimizationModel.t_couriered_flag == "true",
                        ),
                        MStockOptimizationModel.t_couriered_qty,
                    ),
                    else_=0,
                )
            ).label("transferedQty"),
            func.sum(
                case(
                    (
                        and_(
                            and_(
                                MStockOptimizationModel.t_approved_flag == "true",
                                MStockOptimizationModel.t_couriered_flag == "true",
                            ),
                            MStockOptimizationModel.t_received_flag == "true",
                        ),
                        MStockOptimizationModel.t_received_qty,
                    ),
                    else_=0,
                )
            ).label("receivedQty"),
        )
        .select_from(MStockOptimizationModel)
        .filter(and_(*filters))
        .limit(1)
        .one()
    )

    response = {
        "totalTransfers": stats_query.totalTransfers,
        "approvedTransfers": stats_query.approvedTransfers,
        "noOfTransfers": stats_query.noOfTransfers,
        "noOfReceived": stats_query.noOfReceived,
        "suggestedTransferQuantity": stats_query.suggestedTransferQuantity,
        "approvedOptmizedQty": stats_query.approvedOptmizedQty,
        "approvedQty": stats_query.approvedQty,
        "transferedQty": stats_query.transferedQty,
        "receivedQty": stats_query.receivedQty,
    }

    return jsonify(response)

