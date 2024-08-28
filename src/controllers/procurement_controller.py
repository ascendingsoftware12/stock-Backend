from datetime import date, datetime
import random
from flask import Response, request, jsonify, send_file
from io import BytesIO
import pandas as pd
from sqlalchemy import func, or_
from src import db
from src.models.m_stock_optimization_model import MStockOptimizationModel

# -----------------------------------
def get_not_approved_procurement_controller():
    to_store_code = request.args.get("to_store_code")
    model_number = request.args.get("model_number")
    brand = request.args.get("brand")
    state = request.args.get("state")
    item_name = request.args.get('itemname')

    filters = [
        or_(
            MStockOptimizationModel.p_approved_flag == "FALSE",
            MStockOptimizationModel.p_approved_flag == None
        )
    ]

    if to_store_code:
        filters.append(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
    if model_number:
        filters.append(MStockOptimizationModel.MODELNO == model_number)
    if brand:
        filters.append(MStockOptimizationModel.BRAND == brand)
    if item_name:
        filters.append(MStockOptimizationModel.ITEM_NAME == item_name)
    if state:
        states = state.split(',')
        filters.append(MStockOptimizationModel.STATE.in_(states))

    all_stocks = MStockOptimizationModel.query.filter(*filters).all()

    result = [calculate_not_approved_procurement_fields(stock) for stock in all_stocks]
    result.append({
        "id": '',
        "to_store_code" :"",
        "to_store_name":"",
        "model_number":"",
        "state": "",
        "brand":"",
        "item_name":"Total",
        "projection_days":"",
        "po_number":'',
        "po_date":'',
        "p_approved_flag": '',
        "last_28_days_sold_qty":sum(
            stock["last_28_days_sold_qty"] or 0 for stock in result
        ),
        "current_stock":sum(stock["current_stock"] or 0 for stock in result),
        "demand_quantity":sum(stock["demand_quantity"] or 0 for stock in result),
        "transfer_quantity":sum(stock["transfer_quantity"] or 0 for stock in result),
        "yet_to_procure_default":sum(
            stock["yet_to_procure_default"] or 0 for stock in result
        ),
        "yet_to_procure_projected":sum(
            stock["yet_to_procure_projected"] or 0 for stock in result
        ),
        "actual_po_quantity":sum(stock["actual_po_quantity"] or 0 for stock in result),
        # "final_po_quantity": sum(stock["final_po_quantity"] or 0 for stock in result),
       
    })
    
    return jsonify(result)

def calculate_not_approved_procurement_fields(stock):

    # data = request.get_json()
    projecteddaysquery = request.args.get("projecteddays")
    # affordablequantity = request.args.get("affordablequantity")
    last_28_days_sold_qty = stock.DEMAND_STORE_SALES_QTY
    current_stock = stock.DEMAND_STORE_STOCK_QTY
    if(stock.DEMAND_STORE_SALES_QTY and stock.DEMAND_STORE_STOCK_QTY):
        last_28_days_sold_qty = stock.DEMAND_STORE_SALES_QTY
        current_stock = stock.DEMAND_STORE_STOCK_QTY
        demand_quantity = last_28_days_sold_qty - current_stock
    elif(stock.DEMAND_STORE_SALES_QTY and not stock.DEMAND_STORE_STOCK_QTY):
        demand_quantity = stock.DEMAND_STORE_SALES_QTY
    else:
        demand_quantity=0        

    
    
    approved_transfer_quantity = stock.approved_qty if stock.approved_qty is not None else 0
    if stock.approved_qty:
        yet_to_procure_default=demand_quantity - stock.approved_qty
    else:
        yet_to_procure_default=demand_quantity
  

    # form_projected_days = data.get("projecteddays")
    form_projected_days = projecteddaysquery

    if form_projected_days:
        projected_days = int(form_projected_days)
    else:
        if stock.p_projection_days:
            projected_days=stock.p_projection_days
        else:
            projected_days=28
        
    yet_to_procure_projected = round(
            ((last_28_days_sold_qty / 28) * projected_days)
            - approved_transfer_quantity
            - current_stock
        )
    
  
    actual_po_quantity = yet_to_procure_projected
    
    # form_affordable_quantity = data.get("affordable_quantity")
    # form_affordable_quantity = affordablequantity

    # if form_affordable_quantity is not None:
    #     affordable_quantity = int(form_affordable_quantity)
    # else:
    #     affordable_quantity = 1

    total_yet_to_procure_projected_qty = calculate_total_yet_to_procure_projected_qty_for_not_approved()

    # if affordable_quantity is not None and stock.p_approved_flag == "TRUE":
    # if affordable_quantity:
    #     if total_yet_to_procure_projected_qty > 0:
    #             final_po_qty = round(
    #                 (yet_to_procure_projected / float(total_yet_to_procure_projected_qty))
    #                 * affordable_quantity
    #             )
    #     else:
    #         final_po_qty = 0
    # else:
    po_date=""
    if stock.po_date is not None:
        # Parse the date string into a datetime object
        date_obj = datetime.strptime(str(stock.po_date), '%Y-%m-%d')

        # Format the datetime object into DD-MM-YYYY format
        po_date = date_obj.strftime('%d-%m-%Y')
    else:
        po_date=""

    final_po_qty = 0

    return {
        "id": stock.ID_NO,
        "to_store_code": stock.TO_STORE_CODE,
        "to_store_name": stock.TO_STORE_NAME,
        "model_number": stock.MODELNO,
        "state": stock.STATE,
        "brand": stock.BRAND,
        "item_name":stock.ITEM_NAME,
        "last_28_days_sold_qty": last_28_days_sold_qty,
        "current_stock": current_stock,
        "demand_quantity": demand_quantity,
        "transfer_quantity": approved_transfer_quantity,
        "yet_to_procure_default": yet_to_procure_default,
        "yet_to_procure_projected": yet_to_procure_projected,
        "projection_days": projected_days,
        "p_approved_flag": stock.p_approved_flag,
        "actual_po_quantity": actual_po_quantity,        
        "po_number": stock.po_number,
        "po_date": str(po_date),
    }

def calculate_total_yet_to_procure_projected_qty_for_not_approved():
    total_qty = (
        db.session.query(
            func.sum(func.coalesce(MStockOptimizationModel.p_yet_to_procure_projected_qty, 0))
        ).filter(MStockOptimizationModel.p_approved_flag != "TRUE").scalar()
    )
    return total_qty or 0


# -----------------------------------
def get_approved_procurement_controller():
    to_store_code = request.args.get("to_store_code")
    model_number = request.args.get("model_number")
    brand = request.args.get("brand")
    state = request.args.get("state")
    item_name = request.args.get('itemname')

    filters = [
            MStockOptimizationModel.p_approved_flag == "TRUE",
    ]

    if to_store_code:
        filters.append(MStockOptimizationModel.TO_STORE_CODE == to_store_code)
    if model_number:
        filters.append(MStockOptimizationModel.MODELNO == model_number)
    if brand:
        filters.append(MStockOptimizationModel.BRAND == brand)
    if item_name:
        filters.append(MStockOptimizationModel.ITEM_NAME == item_name)
    if state:
        states = state.split(',')
        filters.append(MStockOptimizationModel.STATE.in_(states))

    all_stocks = MStockOptimizationModel.query.filter(*filters).all()

    result = [calculate_approved_procurement_fields(stock) for stock in all_stocks]

    result.append({
        "id": '',
        "to_store_code" :"",
        "to_store_name":"",
        "model_number":"",
        "state": "",
        "brand":"",
        "item_name":"Total",
        "projection_days":"",
        "po_number":'',
        "po_date":'',
        "p_approved_flag": '',
        "last_28_days_sold_qty": sum(
            stock["last_28_days_sold_qty"] or 0 for stock in result
        ) ,
        "current_stock": sum(stock["current_stock"] or 0 for stock in result) ,
        "demand_quantity": sum(stock["demand_quantity"] or 0 for stock in result) ,
        "transfer_quantity":sum(stock["transfer_quantity"] or 0 for stock in result) ,
        "yet_to_procure_default": sum(
            stock["yet_to_procure_default"] or 0 for stock in result
        ) ,
        "yet_to_procure_projected": sum(
            stock["yet_to_procure_projected"] or 0 for stock in result
        ) ,
        "actual_po_quantity": sum(stock["actual_po_quantity"] or 0 for stock in result),
        "final_po_quantity":sum(stock["final_po_quantity"] or 0 for stock in result) ,
       
    })

    return jsonify(result)

def calculate_approved_procurement_fields(stock):
    projecteddaysquery = request.args.get("projecteddays")
    affordable_quantity = request.args.get("affordablequantity")
    
    last_28_days_sold_qty = stock.DEMAND_STORE_SALES_QTY
    current_stock = stock.DEMAND_STORE_STOCK_QTY
    if(stock.DEMAND_STORE_SALES_QTY and stock.DEMAND_STORE_STOCK_QTY):
        last_28_days_sold_qty = stock.DEMAND_STORE_SALES_QTY
        current_stock = stock.DEMAND_STORE_STOCK_QTY
        demand_quantity = last_28_days_sold_qty - current_stock
    elif(stock.DEMAND_STORE_SALES_QTY and not stock.DEMAND_STORE_STOCK_QTY):
        demand_quantity = stock.DEMAND_STORE_SALES_QTY
    else:
        demand_quantity=0        

    approved_transfer_quantity = stock.approved_qty if stock.approved_qty is not None else 0

    if stock.approved_qty:
        yet_to_procure_default=demand_quantity - stock.approved_qty
    else:
        yet_to_procure_default=demand_quantity

    total_yet_to_procure_projected_qty = calculate_total_yet_to_procure_projected_qty_for_approved()
    if stock.p_final_po_qty:
        final_po_qty = stock.p_final_po_qty
       
    else:
        if affordable_quantity:
            if total_yet_to_procure_projected_qty > 0:
                    
                    final_po_qty = round(
                        (stock.p_actual_po_qty / total_yet_to_procure_projected_qty)
                        * int(affordable_quantity)
                    )
                
            else:
                final_po_qty = 0
                
        else:
            final_po_qty = 0  
            
    po_date=""
    if stock.po_date is not None: 
        # Parse the date string into a datetime object
        date_obj = datetime.strptime(str(stock.po_date), '%Y-%m-%d')

        # Format the datetime object into DD-MM-YYYY format
        po_date = date_obj.strftime('%d-%m-%Y')  
    else:
        po_date=""   
    
    return {
        "id": stock.ID_NO,
        "to_store_code": stock.TO_STORE_CODE,
        "to_store_name": stock.TO_STORE_NAME,
        "model_number": stock.MODELNO,
        "state": stock.STATE,
        "brand": stock.BRAND,
        "item_name":stock.ITEM_NAME,
        "last_28_days_sold_qty": stock.DEMAND_STORE_SALES_QTY,
        "current_stock": stock.DEMAND_STORE_STOCK_QTY,
        "demand_quantity": stock.SUPPLIED_QTY,
        "transfer_quantity": stock.approved_qty,
        "yet_to_procure_default": yet_to_procure_default,
        "yet_to_procure_projected": stock.p_yet_to_procure_projected_qty,
        "projection_days": stock.p_projection_days,
        "p_approved_flag": stock.p_approved_flag,
        "actual_po_quantity": stock.p_actual_po_qty,    
        "final_po_quantity": final_po_qty,
        "po_number": stock.po_number,
        "po_date": str(po_date),
        "p_final_po_flag":stock.p_final_po_flag
    
    }

def calculate_total_yet_to_procure_projected_qty_for_approved():
    total_qty = (
        db.session.query(
            func.sum(func.coalesce(MStockOptimizationModel.p_actual_po_qty, 0))
        ).filter(MStockOptimizationModel.p_approved_flag == "TRUE").scalar()
    )
    return total_qty or 0

# -----------------------------------


def save_procurement_controller():
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({"error": "Invalid data format. Expected a list of updates."}), 400
        
        for entry in data:
            stock_id = entry.get("stock_id")
            yet_to_procure_default = entry.get("yet_to_procure_default")
            yet_to_procure_projected = entry.get("yet_to_procure_projected")
            projection_days = entry.get("projection_days")
            # affordable_quantity = entry.get("affordable_quantity")
            
            if stock_id is None:
                return jsonify({"error": "Missing Id in request.", "success": 0}), 400

            stock = MStockOptimizationModel.query.filter_by(ID_NO=stock_id).first()
            if not stock:
                return jsonify({"error": f"Stock with ID {stock_id} not found.", "success": 0}), 404
            
            total_yet_to_procure_projected_qty = calculate_total_yet_to_procure_projected_qty_for_approved()

            # if (stock.p_approved_flag =="TRUE"):
            actual_po_quantity = yet_to_procure_projected
            # else:
            #     actual_po_quantity = 0

            # if affordable_quantity:
            #     if total_yet_to_procure_projected_qty > 0:
            #             final_po_qty = round(
            #                 (yet_to_procure_projected / float(total_yet_to_procure_projected_qty))
            #                 * affordable_quantity
            #             )
            #     else:
            #         final_po_qty = 0
            # else:
            #     final_po_qty = 0
            
            # if yet_to_procure_default is not None:
            #     stock.yet_to_procure_default = yet_to_procure_default
            if yet_to_procure_projected is not None:
                stock.p_yet_to_procure_projected_qty = yet_to_procure_projected
            if projection_days is not None:
                stock.p_projection_days = projection_days
            
            stock.p_approved_flag = "TRUE"
            stock.p_actual_po_qty = actual_po_quantity
            # stock.p_final_po_qty = final_po_qty

            db.session.add(stock)
        
        db.session.commit()

        return jsonify({"success": "Records updated successfully."}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e), "success": 0}), 500



def final_po_save_procurement_controller():
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({"error": "Invalid data format. Expected a list of updates."}), 400
        
        for entry in data:
            stock_id = entry.get("stock_id")
            # projection_days = entry.get("projection_days")
            affordable_quantity = entry.get("affordable_quantity")
            
            if stock_id is None:
                return jsonify({"error": "Missing Id in request.", "success": 0}), 400


            stock = MStockOptimizationModel.query.filter_by(ID_NO=stock_id).first()
            if not stock:
                return jsonify({"error": f"Stock with ID {stock_id} not found.", "success": 0}), 404
            

            yet_to_procure_projected = stock.p_actual_po_qty
            total_yet_to_procure_projected_qty = calculate_total_yet_to_procure_projected_qty_for_approved()

            # if (stock.p_approved_flag =="TRUE"):
            #     actual_po_quantity = yet_to_procure_projected
            # else:
            #     actual_po_quantity = 0

            if affordable_quantity:
                if total_yet_to_procure_projected_qty > 0:
                        final_po_qty = round(
                            (yet_to_procure_projected / float(total_yet_to_procure_projected_qty))
                            * int(affordable_quantity)
                        )
                        
                else:
                    final_po_qty = 0
                    
            else:
                final_po_qty = 0
                
            stock.p_final_po_qty = final_po_qty
            stock.p_final_po_flag= "TRUE"

            db.session.add(stock)
        
        db.session.commit()

        return jsonify({"success": "Records updated successfully."}), 200
    
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e), "success": 0}), 500

# -----------------------------------

def update_projection_procurement_controller():
    try:
        # approved_records = db.session.query(MStockOptimizationModel).filter_by(p_approved_flag="TRUE").all()
        approved_records = MStockOptimizationModel.query.filter(MStockOptimizationModel.p_approved_flag == "TRUE", MStockOptimizationModel.p_final_po_qty != None, MStockOptimizationModel.p_final_po_flag > 0 ,MStockOptimizationModel.po_number == None).all()

        for record in approved_records:
            record.po_number = "PO"+str(datetime.now().strftime('%Y%m%d%H%M%S')) +"-"+str(random.randint(1000,9999))
            record.po_date = date.today()

        db.session.commit()

        return jsonify({"success":1})
        
    except Exception as e:
        return (jsonify({"success": 0, "error": str(e)}), 500)

# -----------------------------------

def approved_export_attaced_excel_procurement__controller():
    response = get_approved_procurement_controller()
    
    if isinstance(response, Response):
        result = response.get_json()
        print(result)
        df = pd.DataFrame(result)
        
        if 'id' in df.columns:
            df = df.drop(columns=['id'])
        
        desired_order = [
            'to_store_code', 'to_store_name', 'model_number', 'state','brand', 'item_name',
            'projection_days', 'po_number', 'po_date', 'p_approved_flag', 
            'last_28_days_sold_qty', 'current_stock', 'demand_quantity', 
            'transfer_quantity', 'yet_to_procure_default', 'yet_to_procure_projected','actual_po_quantity', 
            'final_po_quantity'
        ]
        
        if all(col in df.columns for col in desired_order):
            df = df[desired_order]
        else:
            return "Missing one or more columns in the DataFrame", 200
        
        excel_filename = "REPORT_" + datetime.now().strftime("%Y-%m-%d-%H%M%S") + str(random.randint(100, 999)) + ".xlsx"
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        output.seek(0)
        
        return send_file(output, download_name=excel_filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    return "Invalid response received", 400

def not_approved_export_attaced_excel_procurement__controller():
    response = get_not_approved_procurement_controller()
    
    if isinstance(response, Response):
        result = response.get_json()
        
        df = pd.DataFrame(result)
        
        if 'id' in df.columns:
            df = df.drop(columns=['id'])
        
        desired_order = [
            'to_store_code', 'to_store_name', 'model_number', 'brand', 'item_name',
            'last_28_days_sold_qty', 'current_stock', 'demand_quantity', 
            'transfer_quantity', 'yet_to_procure_default', 'yet_to_procure_projected', 
            'projection_days', 'p_approved_flag'
        ]
        
        if all(col in df.columns for col in desired_order):
            df = df[desired_order]
        else:
            return "Missing one or more columns in the DataFrame", 200
        
        excel_filename = "REPORT_" + datetime.now().strftime("%Y-%m-%d-%H%M%S") + str(random.randint(100, 999)) + ".xlsx"
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        output.seek(0)
        
        return send_file(output, download_name=excel_filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    return "Invalid response received", 400


# Id (ID_NO)
# To store code
# To store name
# Model number
# Brand
# Last 28 days Sold quantity
# Current stock
# Demand quantity
# Transfer quantity
# Yet to procure default
# Yet to procure projected
# Projection days
# P_approved flag
# Actual po quantity
# Final po quantity
# Po number
# Po date


# To Store Code - to_store_code
# To Store Name - to_store_name
# Model Number - modelno
# Brand - brand
# Last 28 Days Sold Quantity - demand_store_sales_qty
# Current Stock - demand_store_stock_qty
# Demand Quantity - last 28days sold-currentStock
# Transfer Quantity - approved_qty
# (if approved_qty is not null then Demand Quantity(use the which is calculated above)-approved_qty else Demand Quantity. There is no column for this. This will always a calculated value or demand_qty)
# Yet to Procure Default - (if p_yet_to_procure_projected_qty is not null in table then use p_yet_to_procure_projected_qty else yet_to_procure_projected = ((last_28Days_sold_Qty/28) projecteddaysinput)-approved TransferQty-current stock; Column name)
# Yet to Procure Projected - p_yet_to_procure_projected_qty
# Projection Days - by Default it should be 28 days if user changes and saves the value then show the saved value. Column name p_projection_days
# Approve p_approved_flag
# Actual PO Quantity - if approved flag true then show Yet to Procure Projected else it will be 0. Column name p_actual_po_qty
# if affordable quantity is not null then (p_yet_to_procure_projected_qty/total yet procure projectedqty) * affordable quantity.
# Final PO Quantity -  p_final_po_qty
# PO Number - po number
# PO Date - po_date
# additional things to be captured are p_approved_by.p_approved_date


# Fetch
# Getprojection
# Save procurement
# GeneratePO
