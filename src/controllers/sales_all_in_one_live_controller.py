from flask import request, jsonify
from sqlalchemy import and_, extract, func
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


def get_sales_all_in_one_live_ytd_controller():
    try:
        pass

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_ytd_controller()
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


def month_name1(month_number):
    months = [
        "Apr", "May", "Jun", "Jul", "Aug", "Sep", 
        "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"
    ]
    return months[(month_number - 4) % 12]


def get_financial_year(year, month):
    if month >= 4:
        return f"{year + 1}"
    else:
        return f"{year - 1}"


# -----------------------------------------------------

# -----------------------------------------------------
# month cr
# -----------------------------------------------------


def get_sales_all_in_one_live_month_cr_controller():
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
