from flask import request, jsonify
from sqlalchemy import and_, case, extract, func
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive

# ----------------------------------------------------------------------------------------------------------
# Get all
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_controller():
    try:
        sales_records = SalesAllInOneLive.query.all()

        sales_data = [
            {
                "invoice_date": record.invoice_date,
                "store_code": record.store_code,
                "store_name": record.store_name,
                "city": record.city,
                "region": record.region,
                "state": record.state,
                "item_code": record.item_code,
                "item_description": record.item_description,
                "actual_item": record.actual_item,
                "brand_name": record.brand_name,
                "product_group": record.product_group,
                "section": record.section,
                "model_no": record.model_no,
                "sales_qty": record.sales_qty,
                "gros_rate": record.gros_rate,
                "disc_amt": record.disc_amt,
                "tax_amt": record.tax_amt,
                "total_sales": record.total_sales,
                "cost_price": record.cost_price,
                "gros_profit": record.gros_profit,
                "revealer_section": record.revealer_section,
                "store_category": record.store_category,
                "store_opened_date": record.store_opened_date,
                "rsm": record.rsm,
                "arsm": record.arsm,
                "asm": record.asm,
                "drsm": record.drsm,
                "branch_type": record.branch_type,
                "franch_type": record.franch_type,
                "srn_flag": record.srn_flag,
                "sale_type": record.sale_type,
                "demo_flag": record.demo_flag,
                "data_refresh_time": record.data_refresh_time,
            }
            for record in sales_records
        ]

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# ytd
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_ytd_cr_controller():
    try:
        pass

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_ytd_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# month
# ----------------------------------------------------------------------------------------------------------


def month_name(month_number):
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    return months[month_number - 1]


# -----------------------------------------------------

# -----------------------------------------------------
# month cr
# -----------------------------------------------------

def get_sales_all_in_one_live_month_cr_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_list = []
        yearly_totals = {}

        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul",
            8: "Aug", 9: "Sep", 10: "Oct",
            11: "Nov", 12: "Dec", 1: "Jan",
            2: "Feb", 3: "Mar"
        }

        for year, month, total_sales in sales_data:
            if month in [1, 2, 3]:
                fiscal_year = year  
            else:
                fiscal_year = year + 1 

            financial_month = month_names[month]
            sales_with_gst = round(total_sales / 10000000, 2)

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = {
                    "FY": str(fiscal_year), 
                    "Total": 0
                }

            yearly_totals[fiscal_year][financial_month] = sales_with_gst
            yearly_totals[fiscal_year]["Total"] += sales_with_gst

        for year, data in yearly_totals.items():
            yearly_totals[year]["Total"] = round(yearly_totals[year]["Total"], 2)

        result_list = list(yearly_totals.values())

        return jsonify(result_list), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_month_cr_controller1():
    try:

        # invoice_date = request.args.get('invoice_date')
        # srn_flag = request.args.get('srn_flag')
        # sales_type = request.args.get('sales_type')
        # section = request.args.get('section')
        # brand_name = request.args.get('brand_name')
        # model_no = request.args.get('model_no')
        # item_description = request.args.get('item_description')

        # conditions = []

        # if invoice_date:
        #     conditions.append(SalesAllInOneLive.invoice_date == invoice_date)

        # if srn_flag:
        #     conditions.append(SalesAllInOneLive.srn_flag == srn_flag)

        # if sales_type:
        #     conditions.append(SalesAllInOneLive.sale_type == sales_type)

        # if section:
        #     conditions.append(SalesAllInOneLive.section == section)

        # if brand_name:
        #     conditions.append(SalesAllInOneLive.brand_name == brand_name)

        # if model_no:
        #     conditions.append(SalesAllInOneLive.model_no == model_no)

        # if item_description:
        #     conditions.append(SalesAllInOneLive.item_description == item_description)

        # .filter(and_(*conditions) if conditions else True)
        
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_dict = {}
        yearly_totals = {}

        for year, month, total_sales in sales_data:
            financial_month = month_name(month)
            sales_with_gst = round(total_sales / 10000000, 2)

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Sales with GST (Cr)": sales_with_gst
            })

            yearly_totals[year] += sales_with_gst

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------


# -----------------------------------------------------
# month cr without gst
# -----------------------------------------------------


def get_sales_all_in_one_live_month_cr_without_gst_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
                func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_dict = {}
        yearly_totals = {}

        for year, month, total_sales, tax_amt in sales_data:
            financial_month = month_name(month)
            sales_without_gst = round((total_sales - tax_amt) / 10000000, 2)

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Sales without GST (Cr)": sales_without_gst
            })

            yearly_totals[year] += sales_without_gst

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_cr_without_gst_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------


# -----------------------------------------------------
# month lk
# -----------------------------------------------------


def get_sales_all_in_one_live_month_lk_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        
        result_dict = {}
        yearly_totals = {}

        for year, month, total_sales in sales_data:
            financial_month = month_name(month)
            sales_lk = round(total_sales / 100000, 2)

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Sales with GST (Lk)": sales_lk,
            })

            yearly_totals[year] += sales_lk

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200
        
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_lk_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------


# -----------------------------------------------------
# month lk without gst
# -----------------------------------------------------


def get_sales_all_in_one_live_month_lk_without_gst_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
                func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_dict = {}
        yearly_totals = {}

        for year, month, total_sales, tax_amt in sales_data:
            financial_month = month_name(month)
            sales_without_gst_lk = round((total_sales - tax_amt) / 10000000, 2)

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Sales without GST (Lk)": sales_without_gst_lk
            })

            yearly_totals[year] += sales_without_gst_lk

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_lk_without_gst_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------

# -----------------------------------------------------
# month sales_qty
# -----------------------------------------------------


def get_sales_all_in_one_live_month_sales_qty_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("sales_qty"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )


        result_dict = {}
        yearly_totals = {}

        for year, month, sales_qty in sales_data:
            financial_month = month_name(month)
            sales_qty = sales_qty

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Sales Qty": sales_qty
            })

            yearly_totals[year] += sales_qty

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_sales_qty_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------

# -----------------------------------------------------
# month total_sales
# -----------------------------------------------------


def get_sales_all_in_one_live_month_total_sales_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        
        result_dict = {}
        yearly_totals = {}

        for year, month, total_sales in sales_data:
            financial_month = month_name(month)
            total_sales = total_sales

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "Total Sales": total_sales
            })

            yearly_totals[year] += total_sales

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_total_sales_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -----------------------------------------------------


# -----------------------------------------------------
# month gp lk
# -----------------------------------------------------


def get_sales_all_in_one_live_month_gp_lk_controller():
    try:
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )


        result_dict = {}
        yearly_totals = {}

        for year, month, gros_profit in sales_data:
            financial_month = month_name(month)
            gros_profit_lk = round(gros_profit / 100000, 2)

            if year not in result_dict:
                result_dict[year] = []
                yearly_totals[year] = 0

            result_dict[year].append({
                "month": financial_month,
                "GP (Lk)": gros_profit_lk
            })

            yearly_totals[year] += gros_profit_lk

        for year, month_data in result_dict.items():
            month_data.append({
                "total": round(yearly_totals[year], 2)  
            })
            
        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200


    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_gp_lk_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------
# product dimension
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_product_dimension_cr_controller():
    try:
        sales_data = (
            db.session.query(
                SalesAllInOneLive.product_group,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                SalesAllInOneLive.product_group,
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )


        result_dict = {}

        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul",
            8: "Aug", 9: "Sep", 10: "Oct",
            11: "Nov", 12: "Dec", 1: "Jan",
            2: "Feb", 3: "Mar"
        }

        for product_group, year, month, total_sales in sales_data:
            
            if month in [1, 2, 3]:
                fiscal_year = year  
            else:
                fiscal_year = year + 1 

            financial_month = month_names[month]
            
            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)

            if product_group not in result_dict:
                result_dict[product_group] = {}

            if fiscal_year not in result_dict[product_group]:
                result_dict[product_group][fiscal_year] = {}

            result_dict[product_group][fiscal_year][financial_month] = sales_with_gst

        return jsonify(result_dict), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_product_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# brand dimension
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_brand_dimension_cr_controller():
    try:
        sales_data = (
            db.session.query(
                SalesAllInOneLive.brand_name,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                SalesAllInOneLive.brand_name,
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_dict = {}

        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul",
            8: "Aug", 9: "Sep", 10: "Oct",
            11: "Nov", 12: "Dec", 1: "Jan",
            2: "Feb", 3: "Mar"
        }

        for brand_name, year, month, total_sales in sales_data:

            if month in [1, 2, 3]:
                fiscal_year = year  
            else:
                fiscal_year = year + 1 

            financial_month = month_names[month]

            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)
            

            if brand_name not in result_dict:
                result_dict[brand_name] = {}

            if fiscal_year not in result_dict[brand_name]:
                result_dict[brand_name][fiscal_year] = {}

            result_dict[brand_name][fiscal_year][financial_month] = sales_with_gst

        return jsonify(result_dict), 200
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_brand_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# item dimension
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_item_dimension_cr_controller():
    try:
        sales_data = (
            db.session.query(
                SalesAllInOneLive.actual_item,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            .group_by(
                SalesAllInOneLive.actual_item,
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .all()
        )

        result_dict = {}

        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul",
            8: "Aug", 9: "Sep", 10: "Oct",
            11: "Nov", 12: "Dec", 1: "Jan",
            2: "Feb", 3: "Mar"
        }

        for actual_item, year, month, total_sales in sales_data:

            if month in [1, 2, 3]:
                fiscal_year = year  
            else:
                fiscal_year = year + 1 

            financial_month = month_names[month]

            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)
            

            if actual_item not in result_dict:
                result_dict[actual_item] = {}

            if fiscal_year not in result_dict[actual_item]:
                result_dict[actual_item][fiscal_year] = {}

            result_dict[actual_item][fiscal_year][financial_month] = sales_with_gst

        return jsonify(result_dict), 200
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------
# price breakup 1
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_price_breakup_one_cr_controller():
    try:
        sales_data = (
            db.session.query(
                SalesAllInOneLive.sales_qty,
                SalesAllInOneLive.total_sales,
                (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty).label("piecewise_sales"),
                case(
                    (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 0, "Null"),
                    ( (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 0) & 
                      (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 5000), "0-5000"),
                    ( (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 5000) & 
                      (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 10000), "5001-10000"),
                    ( (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 10000) & 
                      (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 15000), "10001-15000"),
                    ( (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 15000) & 
                      (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 20000), "15001-20000"),
                    ( (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 20000) & 
                      (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 25000), "20001-25000"),
                    (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 25000, ">25000"),
                    else_="Unknown"  
                ).label("price_breakup")
            )
            .filter(SalesAllInOneLive.sales_qty > 0)  
            .all()
        )

        result = [
            {
                "sales_qty": row.sales_qty,
                "total_sales": row.total_sales,
                "piecewise_sales": round(row.piecewise_sales, 2),
                "price_breakup": row.price_breakup
            }
            for row in sales_data
        ]

        return jsonify(result), 200
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------

