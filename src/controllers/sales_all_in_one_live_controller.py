from flask import request, jsonify
from sqlalchemy import and_, case, extract,or_, func,text
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
from datetime import datetime,timedelta,date
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict
import traceback
from decimal import Decimal,DivisionByZero
import re

# ----------------------------------------------------------------------------------------------------------
# ---------------------------------------- Main Methods ----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_ytd_controller(factor):

    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')

        # Initialize conditions
        conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            conditions.append(SalesAllInOneLive.invoice_date == invoice_date)
        if srn_flag:
            conditions.append(SalesAllInOneLive.srn_flag == srn_flag)
        if sales_type:
            conditions.append(SalesAllInOneLive.sale_type == sales_type)
        if section:
            conditions.append(SalesAllInOneLive.section == section)
        if product_group:
            conditions.append(SalesAllInOneLive.product_group == product_group)
        if item_category:
            conditions.append(SalesAllInOneLive.item_category == item_category)
        if brand_name:
            conditions.append(SalesAllInOneLive.brand_name == brand_name)
        if model_no:
            conditions.append(SalesAllInOneLive.model_no == model_no)
        if item_description:
            conditions.append(SalesAllInOneLive.item_description == item_description)
        if store_name:
            conditions.append(SalesAllInOneLive.store_name == store_name)
        if city:
            conditions.append(SalesAllInOneLive.city == city)
        if demo_flag:
            conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            if price_breakup2 == '0-1000':
                conditions.append(sales_per_unit <= 1000)
            elif price_breakup2 == '1001-2000':
                conditions.append(sales_per_unit.between(1000, 2000))
            elif price_breakup2 == '2001-3000':
                conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
            elif price_breakup2 == '3001-4000':
                conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
            elif price_breakup2 == '4001-5000':
                conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
            elif price_breakup2 == '5001-6000':
                conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
            elif price_breakup2 == '6001-7000':
                conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
            elif price_breakup2 == '7001-8000':
                conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
            elif price_breakup2 == '8001-9000':
                conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
            elif price_breakup2 == '9001-10000':
                conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
            elif price_breakup2 == '10001-20000':
                conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
            elif price_breakup2 == '20001-30000':
                conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
            elif price_breakup2 == '30001-40000':
                conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
            elif price_breakup2 == '40001-50000':
                conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
            elif price_breakup2 == '>50000':
                conditions.append(sales_per_unit > 50000)

        latest_invoice_date = db.session.query(
            func.max(SalesAllInOneLive.invoice_date)
        ).scalar()

        if not latest_invoice_date:
            return jsonify({"success": 0, "error": "No sales data found."}), 404

        latest_year = latest_invoice_date.year
        latest_month = latest_invoice_date.month
        latest_day = latest_invoice_date.day

        start_month = 4  # Fiscal year starts in April
        fiscal_years = [latest_year, latest_year - 1, latest_year - 2, latest_year - 3]
        result = {}

        previous_sales = None  

        for year in fiscal_years:
            start_date = datetime(year, start_month, 1)
            end_date = datetime(year, latest_month, latest_day)

        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
            conditions = []
        

            sales_data = db.session.query(
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
                func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
                func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
                func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            ).filter(
                SalesAllInOneLive.invoice_date >= start_date,
                SalesAllInOneLive.invoice_date <= end_date,
            ).filter(*conditions).all()

            if sales_data and sales_data[0] is not None:
                total_sales, tax_amt, sales_qty, gros_profit = sales_data[0]

                if factor == 'cr':
                    value = 10000000 
                    if total_sales != None:
                        sales_details = round((total_sales) / value, 2)
                    else:
                        sales_details = 0.00
                
                elif factor == 'cr_without_gst':
                    value = 10000000 
                    if total_sales != None and tax_amt != None:
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = 0.00
                
                elif factor == 'lk':
                    value = 100000 
                    if total_sales != None:
                        sales_details = round((total_sales) / value, 2)
                    else:
                        sales_details = 0.00
                
                elif factor == 'lk_without_gst':
                    value = 100000 
                    if total_sales != None and tax_amt != None:
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = 0.00
                
                elif factor == 'sales_qty':
                    if sales_qty != None:
                        sales_details = sales_qty
                    else:
                        sales_details = 0.00
                
                elif factor == 'total_sales':
                    if total_sales != None:
                        sales_details = total_sales
                    else:
                        sales_details = 0.00
                
                elif factor == 'gp':
                    value = 100000 
                    if gros_profit != None:
                        sales_details = round(gros_profit / value, 2)
                    else:
                        sales_details = 0.00

                if previous_sales is not None:
                    if previous_sales != 0:
                        percentage_change = round(
                            ((float(sales_details) - float(previous_sales)) / float(previous_sales)) * 100, 2
                        )
                        sales_with_gst_display = f"{sales_details} ({'+' if percentage_change >= 0 else ''}{percentage_change}%)"
                    else:
                        sales_with_gst_display = f"{sales_details} (0.00%)"
                
                else:
                    sales_with_gst_display = (
                        f"{sales_details} (0.00%)"  
                    )

                result[year + 1] = sales_with_gst_display
                previous_sales = sales_details

        return jsonify(result), 200


    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_ytd_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})




    if factor == "cr":
        return get_sales_all_in_one_live_ytd_cr_controller()
    elif factor == "cr_without_gst":
        pass
    elif factor == "lk":
        pass
    elif factor == "lk_without_gst":
        pass
    elif factor == "sales_qty":
        pass
    elif factor == "total_sales":
        pass
    elif factor == "gp_lk":
        pass


def get_sales_all_in_one_live_monthly_controller(factor):
    
    try:
        
        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),

        ).filter(*conditions)

        sales_data = sales_data.group_by(
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).all()

        result_list = []
        yearly_totals = {}

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        previous_sales = {}

        for year, month, total_sales, tax_amt, sales_qty, gros_profit in sales_data:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]


            previous_month_sales = previous_sales.get(fiscal_year, {}).get(
                month - 1 if month > 1 else 12
            )
            if previous_month_sales is not None:
                if previous_month_sales == 0:
                    previous_month_sales = 1
                percentage_change = round(
                    ((sales_details - previous_month_sales) / previous_month_sales)
                    * 100,
                    2,
                )
                change_display = f"{sales_details} ({'+' if percentage_change >= 0 else ''}{percentage_change}%)"
            else:
                change_display = (
                    f"{sales_details} (0.00%)" 
                )

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = {"FY": str(fiscal_year), "Total": 0}

            yearly_totals[fiscal_year][financial_month] = change_display
            yearly_totals[fiscal_year]["Total"] += sales_details

            if fiscal_year not in previous_sales:
                previous_sales[fiscal_year] = {}
            previous_sales[fiscal_year][month] = sales_details

        for year, data in yearly_totals.items():
            yearly_totals[year]["Total"] = round(yearly_totals[year]["Total"], 2)

        result_list = list(yearly_totals.values())

        return jsonify(result_list), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_monthly_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_weekly_analysis_controller(factor):

    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')

        # Initialize conditions
        conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            conditions.append(SalesAllInOneLive.invoice_date == invoice_date)
        if srn_flag:
            conditions.append(SalesAllInOneLive.srn_flag == srn_flag)
        if sales_type:
            conditions.append(SalesAllInOneLive.sale_type == sales_type)
        if section:
            conditions.append(SalesAllInOneLive.section == section)
        if product_group:
            conditions.append(SalesAllInOneLive.product_group == product_group)
        if item_category:
            conditions.append(SalesAllInOneLive.item_category == item_category)
        if brand_name:
            conditions.append(SalesAllInOneLive.brand_name == brand_name)
        if model_no:
            conditions.append(SalesAllInOneLive.model_no == model_no)
        if item_description:
            conditions.append(SalesAllInOneLive.item_description == item_description)
        if store_name:
            conditions.append(SalesAllInOneLive.store_name == store_name)
        if city:
            conditions.append(SalesAllInOneLive.city == city)
        if demo_flag:
            conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            if price_breakup2 == '0-1000':
                conditions.append(sales_per_unit <= 1000)
            elif price_breakup2 == '1001-2000':
                conditions.append(sales_per_unit.between(1000, 2000))
            elif price_breakup2 == '2001-3000':
                conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
            elif price_breakup2 == '3001-4000':
                conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
            elif price_breakup2 == '4001-5000':
                conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
            elif price_breakup2 == '5001-6000':
                conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
            elif price_breakup2 == '6001-7000':
                conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
            elif price_breakup2 == '7001-8000':
                conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
            elif price_breakup2 == '8001-9000':
                conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
            elif price_breakup2 == '9001-10000':
                conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
            elif price_breakup2 == '10001-20000':
                conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
            elif price_breakup2 == '20001-30000':
                conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
            elif price_breakup2 == '30001-40000':
                conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
            elif price_breakup2 == '40001-50000':
                conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
            elif price_breakup2 == '>50000':
                conditions.append(sales_per_unit > 50000)
        
        fiscal_start_month = 4
        fiscal_start_day = 1

        fiscal_start_date = func.concat(
            func.year(SalesAllInOneLive.invoice_date)
            - case(
                (
                    extract("month", SalesAllInOneLive.invoice_date)
                    < fiscal_start_month,
                    1,
                ),
                else_=0,
            ),
            "-",
            fiscal_start_month,
            "-",
            fiscal_start_day,
        )

        week_number = (
            func.floor(
                func.datediff(SalesAllInOneLive.invoice_date, fiscal_start_date) / 7
            )
            + 1
        )

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        

        weekly_sales = db.session.query(
            week_number.label("week_number"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        weekly_sales = (
            weekly_sales.group_by(
                week_number,
                extract("year", SalesAllInOneLive.invoice_date),
                extract("month", SalesAllInOneLive.invoice_date),
            )
            .order_by(week_number)
            .all()
        )

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        result_dict = {}
        years_list = []
        yearly_totals = {}

        for week_number, month, year, total_sales, tax_amt, sales_qty, gros_profit in weekly_sales:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)

            if week_number > 52:
                continue

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            week_label = f"Week {int(week_number):02}"

            if week_label not in result_dict:
                result_dict[week_label] = {}

            result_dict[week_label][fiscal_year] = {"sales_details": sales_details}

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = 0
            yearly_totals[fiscal_year] += sales_details

        for week_label, year_data in result_dict.items():
            for fiscal_year, data in year_data.items():
                yearly_total = yearly_totals.get(fiscal_year, 0)
                if yearly_total > 0:
                    percentage = round((data["sales_details"] / yearly_total) * 100, 2)
                    result_dict[week_label][fiscal_year]["percentage"] = percentage

                    if data["sales_details"] == 0:
                        result_dict[week_label][
                            fiscal_year
                        ] = f"{data['sales_details']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_details"] / yearly_total) * 100, 2
                        )
                        # result_dict[week_label][fiscal_year]["percentage"] = percentage
                        result_dict[week_label][
                            fiscal_year
                        ] = f"{data['sales_details']} ({percentage}%)"
                else:
                    result_dict[week_label][
                            fiscal_year
                        ] = f"{data['sales_details']} ({0.00}%)"

        years_list.sort(reverse=True)

        return jsonify({"values": result_dict, "years": years_list}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_weekly_analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_day_analysis_controller(factor):
    
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')

        # Initialize conditions
        conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            conditions.append(SalesAllInOneLive.invoice_date == invoice_date)
        if srn_flag:
            conditions.append(SalesAllInOneLive.srn_flag == srn_flag)
        if sales_type:
            conditions.append(SalesAllInOneLive.sale_type == sales_type)
        if section:
            conditions.append(SalesAllInOneLive.section == section)
        if product_group:
            conditions.append(SalesAllInOneLive.product_group == product_group)
        if item_category:
            conditions.append(SalesAllInOneLive.item_category == item_category)
        if brand_name:
            conditions.append(SalesAllInOneLive.brand_name == brand_name)
        if model_no:
            conditions.append(SalesAllInOneLive.model_no == model_no)
        if item_description:
            conditions.append(SalesAllInOneLive.item_description == item_description)
        if store_name:
            conditions.append(SalesAllInOneLive.store_name == store_name)
        if city:
            conditions.append(SalesAllInOneLive.city == city)
        if demo_flag:
            conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            if price_breakup2 == '0-1000':
                conditions.append(sales_per_unit <= 1000)
            elif price_breakup2 == '1001-2000':
                conditions.append(sales_per_unit.between(1000, 2000))
            elif price_breakup2 == '2001-3000':
                conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
            elif price_breakup2 == '3001-4000':
                conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
            elif price_breakup2 == '4001-5000':
                conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
            elif price_breakup2 == '5001-6000':
                conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
            elif price_breakup2 == '6001-7000':
                conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
            elif price_breakup2 == '7001-8000':
                conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
            elif price_breakup2 == '8001-9000':
                conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
            elif price_breakup2 == '9001-10000':
                conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
            elif price_breakup2 == '10001-20000':
                conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
            elif price_breakup2 == '20001-30000':
                conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
            elif price_breakup2 == '30001-40000':
                conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
            elif price_breakup2 == '40001-50000':
                conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
            elif price_breakup2 == '>50000':
                conditions.append(sales_per_unit > 50000)

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        
        
        sales_data = db.session.query(
            func.date_format(SalesAllInOneLive.invoice_date, "%M").label("month"),
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            func.week(SalesAllInOneLive.invoice_date).label("week"),
            func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        sales_data = (
            sales_data.group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                func.week(SalesAllInOneLive.invoice_date),
                func.dayofweek(SalesAllInOneLive.invoice_date),
            )
            .order_by(extract("year", SalesAllInOneLive.invoice_date).desc())
            .all()
        )

        formatted_data = []

        day_mapping = {
            1: "mon",
            2: "tue",
            3: "wed",
            4: "thu",
            5: "fri",
            6: "sat",
            7: "sun",
        }

        grouped_sales = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "year": None,
                        "month": None,
                        "week": None,
                        "mon": {"sales_details": "-", "percentage": "-"},
                        "tue": {"sales_details": "-", "percentage": "-"},
                        "wed": {"sales_details": "-", "percentage": "-"},
                        "thu": {"sales_details": "-", "percentage": "-"},
                        "fri": {"sales_details": "-", "percentage": "-"},
                        "sat": {"sales_details": "-", "percentage": "-"},
                        "sun": {"sales_details": "-", "percentage": "-"},
                    }
                )
            )
        )

        weekly_totals = defaultdict(lambda: defaultdict(float))

        for month, year, week, day_of_week, total_sales, tax_amt, sales_qty, gros_profit in sales_data:


            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)


            fiscal_year = year if month in [1, 2, 3] else year + 1
            day_key = day_mapping[day_of_week]
            week_key = f"Week{week}"
            # print(fiscal_year)

            # sales_with_gst = round(total_sales / 10000000, 2)
            grouped_sales[fiscal_year][month][week_key][day_key][
                "sales_details"
            ] = sales_details

            # print(sales_details)
            weekly_totals[fiscal_year][week_key] += float(sales_details)

        for fiscal_year, months in grouped_sales.items():
            for month, weeks in months.items():
                for week, sales in weeks.items():
                    weekly_total = weekly_totals[fiscal_year][week]
                    for day in day_mapping.values():
                        if weekly_total > 0:
                            if sales[day]["sales_details"] != "-":
                                percentage = round(
                                    (float(sales[day]["sales_details"]) / weekly_total) * 100,
                                    2,
                                )
                                sales[day] = (
                                    f"{sales[day]['sales_details']} ({percentage}%)"
                                )
                                # sales[day]["percentage"] = percentage
                            else:
                                sales[day] = f"{0.00} ({0.00}%)"
                        else:
                            sales[day] = f"{0.00} ({0.00}%)"

                    sales["year"] = str(fiscal_year)
                    sales["month"] = month
                    sales["week"] = week

                    formatted_data.append(sales)

        # for entry in formatted_data:
        #     # print(entry)
        return jsonify(formatted_data), 200

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_day_analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_product_dimension_controller(factor):

    try:

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            SalesAllInOneLive.product_group,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            SalesAllInOneLive.product_group,
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).all()

        result_dict = {}

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        years_list = []
        yearly_totals = {}

        for product_group, year, month, total_sales, tax_amt, sales_qty, gros_profit in sales_data:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)
            # sales_with_gst = round(total_sales / 10000000, 2)

            if product_group not in result_dict:
                result_dict[product_group] = {}

            if fiscal_year not in result_dict[product_group]:
                result_dict[product_group][fiscal_year] = {}

            if product_group not in yearly_totals:
                yearly_totals[product_group] = {}

            if fiscal_year not in yearly_totals[product_group]:
                yearly_totals[product_group][fiscal_year] = 0

            yearly_totals[product_group][fiscal_year] += sales_details

            result_dict[product_group][fiscal_year][financial_month] = {
                "sales_details": sales_details,
            }

        for product_group, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[product_group][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():
                    if data["sales_details"] == 0:
                        result_dict[product_group][fiscal_year][
                            month
                        ] = f"{data['sales_details']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_details"] / yearly_total) * 100, 2
                        )
                        result_dict[product_group][fiscal_year][
                            month
                        ] = f"{data['sales_details']} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_product_dimension_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_brand_dimension_controller(factor):

    try:

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            SalesAllInOneLive.brand_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            SalesAllInOneLive.brand_name,
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).all()

        result_dict = {}

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        years_list = []
        yearly_totals = {}

        for brand_name, year, month, total_sales, tax_amt, sales_qty, gros_profit in sales_data:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)

            if brand_name not in result_dict:
                result_dict[brand_name] = {}

            if fiscal_year not in result_dict[brand_name]:
                result_dict[brand_name][fiscal_year] = {}

            if brand_name not in yearly_totals:
                yearly_totals[brand_name] = {}

            if fiscal_year not in yearly_totals[brand_name]:
                yearly_totals[brand_name][fiscal_year] = 0

            # result_dict[brand_name][fiscal_year][financial_month] = sales_with_gst

            yearly_totals[brand_name][fiscal_year] += sales_details

            result_dict[brand_name][fiscal_year][financial_month] = {
                "sales_details": sales_details,
            }

        for brand_name, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[brand_name][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():
                    if data["sales_details"] == 0:
                        result_dict[brand_name][fiscal_year][
                            month
                        ] = f"{data['sales_details']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_details"] / yearly_total) * 100, 2
                        )
                        # result_dict[brand_name][fiscal_year][month]["percentage"] = percentage
                        result_dict[brand_name][fiscal_year][
                            month
                        ] = f"{data['sales_details']} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_brand_dimension_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_item_dimension_controller(factor):

    try:

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            SalesAllInOneLive.actual_item,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)
       
        sales_data = sales_data.group_by(
            SalesAllInOneLive.actual_item,
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).all()

        result_dict = {}

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        years_list = []
        yearly_totals = {}

        for actual_item, year, month, total_sales, tax_amt, sales_qty, gros_profit in sales_data:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)

            if actual_item is None or actual_item == "":
                actual_item="Emp"  

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)

            if actual_item not in result_dict:
                result_dict[actual_item] = {}

            if fiscal_year not in result_dict[actual_item]:
                result_dict[actual_item][fiscal_year] = {}

            if actual_item not in yearly_totals:
                yearly_totals[actual_item] = {}

            if fiscal_year not in yearly_totals[actual_item]:
                yearly_totals[actual_item][fiscal_year] = 0


            yearly_totals[actual_item][fiscal_year] += sales_details

            result_dict[actual_item][fiscal_year][financial_month] = {
                "sales_details": sales_details,
            }
        max_sales_with_gst = 0
        for actual_item, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[actual_item][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():

                    data_sales_with_gst = data["sales_details"]

                    if data_sales_with_gst > max_sales_with_gst:
                        max_sales_with_gst = data_sales_with_gst

                    if data["sales_details"] <= 0:
                        result_dict[actual_item][fiscal_year][
                            month
                        ] = f"{data['sales_details']} ({0.00}%)"
                    else:
                        if yearly_total > 0: # fix: for gp --> error( / 0 )
                            percentage = round(
                                (data["sales_details"] / yearly_total) * 100, 2 
                            ) 
                            result_dict[actual_item][fiscal_year][
                                month
                            ] = f"{data['sales_details']} ({percentage}%)"
                        else:
                            result_dict[actual_item][fiscal_year][
                                month
                            ] = f"{data['sales_details']} ({0.00}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict, "max": max_sales_with_gst}), 200

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_price_breakup_one_controller(factor):

    try:

        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            extract("year", SalesAllInOneLive.invoice_date)
        ).all()

        result_dict = {}

        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }

        price_ranges = {
            "0 - 5000": {},
            "5001 - 10000": {},
            "10001 - 15000": {},
            "15001 - 20000": {},
            "20001 - 25000": {},
            ">25000": {},
        }

        total_sales_by_year = defaultdict(float)
        years_set = set()

        for year, month, total_sales, total_qty, tax_amt, sales_qty, gros_profit in sales_data:

            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)


            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]
            years_set.add(fiscal_year)

            piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
            # sales_with_gst = round(total_sales / 10000000, 2)

            total_sales_by_year[fiscal_year] += float(sales_details)

            price_breakup = "Null"
            if piecewise_sales > 0 and piecewise_sales <= 5000:
                price_breakup = "0 - 5000"
            elif piecewise_sales > 5000 and piecewise_sales <= 10000:
                price_breakup = "5001 - 10000"
            elif piecewise_sales > 10000 and piecewise_sales <= 15000:
                price_breakup = "10001 - 15000"
            elif piecewise_sales > 15000 and piecewise_sales <= 20000:
                price_breakup = "15001 - 20000"
            elif piecewise_sales > 20000 and piecewise_sales <= 25000:
                price_breakup = "20001 - 25000"
            elif piecewise_sales > 25000:
                price_breakup = ">25000"

            if price_breakup != "Null":
                if fiscal_year not in price_ranges[price_breakup]:
                    price_ranges[price_breakup][fiscal_year] = 0
                price_ranges[price_breakup][fiscal_year] += sales_details

        years_list = sorted(years_set, reverse=True)

        for price_range, sales_data in price_ranges.items():
            for year in years_list:
                sales_value = sales_data.get(year, 0)
                total_sales = total_sales_by_year[year]
                percentage = (
                    round((float(sales_value) / total_sales) * 100, 2)
                    if total_sales > 0
                    else 0
                )
                # sales_data[year] = {
                #     "sales_details": sales_value,
                #     "percentage": percentage
                # }

                sales_data[year] = f"{sales_value} ({percentage}%)"

        return jsonify({"years": years_list, "values": price_ranges}), 200

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_price_breakup_two_controller(factor):

    try:
        
        conditions = search_sales_all_in_one_controller()
        if not isinstance(conditions, list):
    # Safeguard in case the function did not return a list
         conditions = []
        

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            extract("year", SalesAllInOneLive.invoice_date)
        ).all()

        result_dict = {}

        price_ranges = {
            "0 - 1000": {},
            "1001 - 2000": {},
            "2001 - 3000": {},
            "3001 - 4000": {},
            "4001 - 5000": {},
            "5001 - 6000": {},
            "6001 - 7000": {},
            "7001 - 8000": {},
            "8001 - 9000": {},
            "9001 - 10000": {},
            "10001 - 20000": {},
            "20001 - 30000": {},
            "30001 - 40000": {},
            "40001 - 50000": {},
            ">50000": {},
        }

        years_set = set()

        total_sales_by_year = defaultdict(float)

        for year, month, total_sales, total_qty, tax_amt, sales_qty, gros_profit in sales_data:
            
            if factor == 'cr':
                value = 10000000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round((total_sales) / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)


            piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
            # sales_with_gst = round(total_sales / 10000000, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            years_set.add(fiscal_year)

            total_sales_by_year[fiscal_year] += float(sales_details)

            price_breakup = "Null"
            if piecewise_sales > 0 and piecewise_sales <= 1000:
                price_breakup = "0 - 1000"
            elif piecewise_sales > 1000 and piecewise_sales <= 2000:
                price_breakup = "1001 - 2000"
            elif piecewise_sales > 2000 and piecewise_sales <= 3000:
                price_breakup = "2001 - 3000"
            elif piecewise_sales > 3000 and piecewise_sales <= 4000:
                price_breakup = "3001 - 4000"
            elif piecewise_sales > 4000 and piecewise_sales <= 5000:
                price_breakup = "4001 - 5000"
            elif piecewise_sales > 5000 and piecewise_sales <= 6000:
                price_breakup = "5001 - 6000"
            elif piecewise_sales > 6000 and piecewise_sales <= 7000:
                price_breakup = "6001 - 7000"
            elif piecewise_sales > 7000 and piecewise_sales <= 8000:
                price_breakup = "7001 - 8000"
            elif piecewise_sales > 8000 and piecewise_sales <= 9000:
                price_breakup = "8001 - 9000"
            elif piecewise_sales > 9000 and piecewise_sales <= 10000:
                price_breakup = "9001 - 10000"
            elif piecewise_sales > 10000 and piecewise_sales <= 20000:
                price_breakup = "10001 - 20000"
            elif piecewise_sales > 20000 and piecewise_sales <= 30000:
                price_breakup = "20001 - 30000"
            elif piecewise_sales > 30000 and piecewise_sales <= 40000:
                price_breakup = "30001 - 40000"
            elif piecewise_sales > 40000 and piecewise_sales <= 50000:
                price_breakup = "40001 - 50000"
            elif piecewise_sales > 50000:
                price_breakup = ">50000"

            if price_breakup != "Null":
                if fiscal_year not in price_ranges[price_breakup]:
                    price_ranges[price_breakup][fiscal_year] = 0
                price_ranges[price_breakup][fiscal_year] += sales_details

        years_list = sorted(years_set, reverse=True)

        for price_range, sales_data in price_ranges.items():
            for year in years_list:
                sales_value = sales_data.get(year, 0)
                total_sales = total_sales_by_year[year]
                percentage = (
                    round((float(sales_value) / total_sales) * 100, 2)
                    if total_sales > 0
                    else 0
                )
                # sales_data[year] = {
                #     "sales_details": sales_value,
                #     "percentage": percentage
                # }

                sales_data[year] = f"{sales_value} ({percentage}%)"

        return jsonify({"years": years_list, "values": price_ranges}), 200

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_two_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------- Main Methods (END) ---------------------------------------------
# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------- Utility Functions  ---------------------------------------------
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


def get_unique_invoice_dates_controller():
    try:
        result = db.session.query(SalesAllInOneLive.invoice_date.distinct()).all()
        result_list = [row[0] for row in result]

        return jsonify(result_list)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_unique_invoice_dates_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_unique_sale_types_controller():
    try:
        result = db.session.query(SalesAllInOneLive.sale_type.distinct()).all()
        result_list = [row[0] for row in result]

        return jsonify(result_list)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_unique_sale_types_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_unique_srn_flags_controller():
    try:
        result = db.session.query(SalesAllInOneLive.srn_flag.distinct()).all()
        result_list = [row[0] for row in result]

        return jsonify(result_list)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_unique_srn_flags_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_itemsdesc_brand_model_and_section_controller():
    try:
    
        brand_name = request.args.get("brand_name")
        model_no = request.args.get("model_no")
        section = request.args.get("section")
        item_description = request.args.get("item_description")
    
        query = db.session.query(
                SalesAllInOneLive.brand_name,
                SalesAllInOneLive.model_no,
                SalesAllInOneLive.section,
                SalesAllInOneLive.item_description,
            )
            
        if brand_name and brand_name != '':
            query = query.filter(SalesAllInOneLive.brand_name == brand_name)
        if model_no and model_no != '':
            query = query.filter(SalesAllInOneLive.model_no == model_no)
        if section and section != '':
            query = query.filter(SalesAllInOneLive.section == section)
        if item_description and item_description != '':
            query = query.filter(SalesAllInOneLive.item_description == item_description)

        result = query.distinct().all()
        
        brand_names = list(set(row.brand_name for row in result if row.brand_name))
        model_nos = list(set(row.model_no for row in result if row.model_no))
        sections = list(set(row.section for row in result if row.section))
        item_descriptions = list(set(row.item_description for row in result if row.item_description))

        response_data = {
            "brand_names": brand_names,
            "model_nos": model_nos,
            "sections": sections,
            "item_descriptions": item_descriptions
        }

        return jsonify(response_data)
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_itemsdesc_brand_model_and_section_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def search_sales_all_in_one_controller():
    try:
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        gstfilter = request.args.get('gstfilter')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        conditions = []

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)

        elif period_from and period_from != '':
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        
        elif period_to and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)

        elif invoice_date and invoice_date != '':
            conditions.append(SalesAllInOneLive.invoice_date == invoice_date)

        elif srn_flag and srn_flag != '':
            conditions.append(SalesAllInOneLive.srn_flag == srn_flag)

        elif sales_type and sales_type != '':
            conditions.append(SalesAllInOneLive.sale_type == sales_type)

        elif section and section != '':
            conditions.append(SalesAllInOneLive.section == section)

        elif product_group and product_group != '':
            conditions.append(SalesAllInOneLive.product_group == product_group)

        elif item_category and item_category != '':
            conditions.append(SalesAllInOneLive.item_category == item_category)

        elif brand_name and brand_name != '':
            conditions.append(SalesAllInOneLive.brand_name == brand_name)

        elif model_no and model_no != '':
            conditions.append(SalesAllInOneLive.model_no == model_no)

        elif item_description and item_description != '':
            conditions.append(SalesAllInOneLive.item_description == item_description)

        elif store_name and store_name != '':
            conditions.append(SalesAllInOneLive.store_name == store_name)

        elif city and city != '':
            conditions.append(SalesAllInOneLive.city == city)

        elif demo_flag and demo_flag != '':
            conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

        # elif gstfilter and gstfilter != '' and gstfilter == 'withGSTCr':
        #     conditions.append(SalesAllInOneLive.total_sales/10000000)

        # elif gstfilter and gstfilter != '' and gstfilter=='withoutGSTCR':
        #     conditions.append((SalesAllInOneLive.total_sales -SalesAllInOneLive.tax_amt) / 10000000)

        # elif gstfilter and gstfilter != '' and gstfilter=='withGSTLk':
        #     conditions.append(SalesAllInOneLive.total_sales/100000)

        # elif gstfilter and gstfilter != '' and gstfilter=='withoutGSTLk':
        #      total_sales_without_gst = ((SalesAllInOneLive.total_sales - SalesAllInOneLive.tax_amt) / 100000).label("total_sales1")
        #      conditions.append(total_sales_without_gst) 

        # elif gstfilter and gstfilter != '' and gstfilter=='salesqty':
        #     conditions.append(SalesAllInOneLive.sales_qty)

        # elif gstfilter and gstfilter != '' and gstfilter=='totalsales':
        #     conditions.append(SalesAllInOneLive.total_sales)
        
        # elif gstfilter and gstfilter != '' and gstfilter=='gpLk':
        #     conditions.append(SalesAllInOneLive. gros_profit/100000)

        elif price_breakup2 and price_breakup2 != '':
            if price_breakup2 == '0-1000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 1000)
            elif price_breakup2 == '1001-2000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty.between(1000,2000))
            elif price_breakup2 == '2001-3000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 2000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 3000)
            elif price_breakup2 == '3001-4000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 3000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 4000)
            elif price_breakup2 == '4001-5000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 4000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 5000)
            elif price_breakup2 == '5001-6000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 5000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 6000)
            elif price_breakup2 == '6001-7000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 6000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 7000)
            elif price_breakup2 == '7001-8000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 7000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 8000)
            elif price_breakup2 == '8001-9000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 8000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 9000)
            elif price_breakup2 == '9001-10000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 9000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 10000)
            elif price_breakup2 == '10001-20000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 10000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 20000)
            elif price_breakup2 == '20001-30000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 20000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 30000)
            elif price_breakup2 == '30001-40000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 30000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 40000)
            elif price_breakup2 == '40001-50000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 40000 & SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 50000)
            elif price_breakup2 == '>50000':
                conditions.append(SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 50000)
        else:
                conditions.append((SalesAllInOneLive.total_sales).label("total_sales"))
                # print(conditions)

        return conditions
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_sales_all_in_one_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# ----------------------------------------------------------------------------------------------------------
# --------------------------------------- Utility Functions (END) ------------------------------------------
# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# ------------------------------------------------ CRUD ----------------------------------------------------
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
# --------------------------------------------- CRUD (END) -------------------------------------------------
# ----------------------------------------------------------------------------------------------------------


# -------------------------------------------- YTD -------------------------------------------

def get_sales_all_in_one_live_ytd_cr_controller():
    try:
        factor = request.args.get('gstfilter', 'cr')  
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Apply the factor logic
        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately, as sales_qty doesn't need a factor
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

 
        previous_sales = None  # Track previous year's sales to calculate YoY percentage
        latest_invoice_date = db.session.query(
            func.max(SalesAllInOneLive.invoice_date)
        ).scalar()

        if not latest_invoice_date:
            return jsonify({"success": 0, "error": "No sales data found."}), 404

        latest_year = latest_invoice_date.year
        latest_month = latest_invoice_date.month
        latest_day = latest_invoice_date.day
        fiscal_year_start_month = 4  # Fiscal year starts in April

        # Determine the latest fiscal year
        if latest_month < fiscal_year_start_month:
            latest_fiscal_year = latest_year - 1
        else:
            latest_fiscal_year = latest_year

        # Generate the list of fiscal years (latest 4 fiscal years)
        fiscal_years = [latest_fiscal_year - i for i in range(4)]
        fiscal_years_reversed = list(reversed(fiscal_years))

        result = {}

        # Ensure latest_invoice_date is a datetime object
        if isinstance(latest_invoice_date, date) and not isinstance(latest_invoice_date, datetime):
            latest_invoice_date = datetime.combine(latest_invoice_date, datetime.min.time())

        previous_sales = {}  # Initialize as a dictionary to track fiscal year and year-specific sales
        percentage_change=0
        for year in fiscal_years_reversed:
            # Calculate start and end dates for the fiscal year dynamically
            start_date = datetime(year, fiscal_year_start_month, 1)
            fiscal_year_end_date = datetime(year + 1, latest_month, latest_day)
            end_date = min(fiscal_year_end_date, latest_invoice_date)

            # Query to calculate total sales for the YTD period
            total_sales, sales_qty, tax_amt, gros_profit = (
                db.session.query(
                    func.sum(SalesAllInOneLive.total_sales).label('total_amount'),
                    func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
                    func.sum(SalesAllInOneLive.tax_amt).label('tax_amt'),
                    func.sum(SalesAllInOneLive.gros_profit).label('gros_profit')
                )
                .filter(
                    SalesAllInOneLive.invoice_date >= start_date,
                    SalesAllInOneLive.invoice_date <= end_date,
                )
                .filter(*conditions)
                .one_or_none()
            )

            if total_sales is None:
                total_sales = 0
                sales_qty = 0
                tax_amt = 0
                gros_profit = 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            # Retrieve sales from the previous fiscal year for YoY comparison
            previous_fiscal_year = year+1
            previous_month_sales = previous_sales.get(previous_fiscal_year-2, 0)
            

            # Calculate percentage change
            if previous_month_sales > 0:
                percentage_change = round(((sales_with_gst - previous_month_sales) / previous_month_sales) * 100,2)
            
            sales_with_gst_display = f"{sales_with_gst} ({'+' if percentage_change > 0 else ''}{percentage_change}%)"
            
            # Store the current year's sales for future comparison
            previous_sales[year] = sales_with_gst
            previous_month_sales=sales_with_gst
            # Store result with YoY change
            result[year + 1] = sales_with_gst_display

        
        sorted_result = dict(sorted(result.items(), reverse=True))
    
        return jsonify(sorted_result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_ytd_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# --------------------------------------------- Month ----------------------------------------

# Predefine month names
MONTH_NAMES = {
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
    1: "Jan",
    2: "Feb",
    3: "Mar",
}

def get_sales_all_in_one_live_month_cr_controller():
    try:
        # Extract query parameters
        factor = request.args.get('gstfilter', 'cr')
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic filters
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Factor processing
        factor_map = {
            'cr': 10000000,
            'cr_without_gst': 10000000,
            'lk': 100000,
            'lk_without_gst': 100000,
            'gp': 100000,
        }
        value = factor_map.get(factor, None)
        if factor not in factor_map and factor not in ['sales_qty', 'total_sales']:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400
        
        # Query for sales data
        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            func.sum(SalesAllInOneLive.tax_amt).label('tax_amt'),
            func.sum(SalesAllInOneLive.gros_profit).label('gros_profit')
        ).filter(*conditions).group_by(
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date)
        ).all()

        # Process results efficiently
        yearly_totals = {}
        previous_sales = {}

        for row in sales_data:
            year, month, total_sales, sales_qty, tax_amt, gros_profit = row
            fiscal_year = year if month in [1, 2, 3] else year + 1
            financial_month = MONTH_NAMES[month]
            # Default values for None fields
            total_sales = total_sales or 0
            sales_qty = sales_qty or 0
            tax_amt = tax_amt or 0
            gros_profit = gros_profit or 0

            # Calculate sales details based on factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            elif factor in ['cr', 'cr_without_gst']:
                sales_details = round((total_sales - tax_amt) / value, 2) if 'without_gst' in factor else round(total_sales / value, 2)
            elif factor in ['lk', 'lk_without_gst']:
                sales_details = round((total_sales - tax_amt) / value, 2) if 'without_gst' in factor else round(total_sales / value, 2)

            sales_with_gst = sales_details

            # Previous fiscal year calculations
            previous_fiscal_year = fiscal_year - 1
            previous_month_sales = previous_sales.get(previous_fiscal_year, {}).get(month, 0)

            # Calculate percentage change
            percentage_change = round(((sales_with_gst - previous_month_sales) / previous_month_sales) * 100, 2) if previous_month_sales else 0

            change_display = f"{sales_with_gst} ({'+' if percentage_change >= 0 else ''}{percentage_change}%)"

            yearly_totals.setdefault(fiscal_year, {"FY": str(fiscal_year), "Total": 0})
            yearly_totals[fiscal_year][financial_month] = change_display
            yearly_totals[fiscal_year]["Total"] += sales_with_gst

            previous_sales.setdefault(fiscal_year, {})[month] = sales_with_gst

        # Finalize yearly totals
        for year, data in yearly_totals.items():
            data["Total"] = round(data["Total"], 2)

        result_list = list(yearly_totals.values())
        return jsonify(result_list), 200

    except SQLAlchemyError as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_cr_controller()
        return jsonify({"success": 0, "error": "Database Error: " + str(e)}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": 0, "error": str(e)}), 500


# ----------------------------------------- Weekly Analysis ----------------------------------

# def get_sales_all_in_one_live_weekly_analysis_cr_controller():
#     try:
#         fiscal_start_month = 4
#         fiscal_start_day = 1

#         fiscal_start_date = func.concat(
#             func.year(SalesAllInOneLive.invoice_date)
#             - case(
#                 (
#                     extract("month", SalesAllInOneLive.invoice_date)
#                     < fiscal_start_month,
#                     1,
#                 ),
#                 else_=0,
#             ),
#             "-",
#             fiscal_start_month,
#             "-",
#             fiscal_start_day,
#         )

#         week_number = (
#             func.floor(
#                 func.datediff(SalesAllInOneLive.invoice_date, fiscal_start_date) / 7
#             )
#             + 1
#         )

#         # Determine the factor from request arguments
#         factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

#         # Define the logic for sales details based on the factor
#         if factor in ["cr", "cr_without_gst", "lk", "lk_without_gst", "gp"]:
#             value = 10000000 if "cr" in factor else 100000
#         else:
#             value = 1  # Default scaling factor for raw values

#         if factor == "cr":
#             sales_details_field = func.round(func.sum(SalesAllInOneLive.total_sales) / value, 2)
#         elif factor == "cr_without_gst":
#             sales_details_field = func.round(
#                 (func.sum(SalesAllInOneLive.total_sales) - func.sum(SalesAllInOneLive.tax_amt)) / value, 2
#             )
#         elif factor == "lk":
#             sales_details_field = func.round(func.sum(SalesAllInOneLive.total_sales) / value, 2)
#         elif factor == "lk_without_gst":
#             sales_details_field = func.round(
#                 (func.sum(SalesAllInOneLive.total_sales) - func.sum(SalesAllInOneLive.tax_amt)) / value, 2
#             )
#         elif factor == "sales_qty":
#             sales_details_field = func.sum(SalesAllInOneLive.sales_qty)
#         elif factor == "gp":
#             sales_details_field = func.round(func.sum(SalesAllInOneLive.gros_profit) / value, 2)
#         else:
#             sales_details_field = func.sum(SalesAllInOneLive.total_sales)  # Default

#         # Pagination
#         page = request.args.get("page", 1, type=int)
#         limit = request.args.get("limit", 10, type=int)
#         offset = (page - 1) * limit

#         period_from = request.args.get("period_from")
#         period_to = request.args.get("period_to")
#         invoice_date = request.args.get('invoice_date')
#         srn_flag = request.args.get('srn_flag')
#         sales_type = request.args.get('sales_type')
#         section = request.args.get('section')
#         product_group = request.args.get('product_group')
#         item_category = request.args.get('item_category')
#         brand_name = request.args.get('brand_name')
#         model_no = request.args.get('model_no')
#         item_description = request.args.get('item_description')
#         store_name = request.args.get('store_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         asm = request.args.get('asm')
#         store_code = request.args.get('storecode')

#         # Initialize conditions
#         conditions = []
#         price_conditions = []

#         # Apply dynamic conditions
#         if asm:
#             conditions.append(SalesAllInOneLive.asm == asm)
#         if store_code:
#             store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
#             conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
#         if period_from:
#             conditions.append(SalesAllInOneLive.invoice_date >= period_from)
#         if period_to:
#             conditions.append(SalesAllInOneLive.invoice_date <= period_to)
#         if invoice_date:
#             invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
#             conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
#         if srn_flag:
#             srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
#             conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
#         if sales_type:
#             sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
#             conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
#         if section:
#             section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
#             conditions.append(SalesAllInOneLive.section.in_(section_list))
#         if product_group:
#             product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
#             conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
#         if item_category:
#             item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
#             conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
#         if brand_name:
#             brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
#             conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
#         if model_no:
#             model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
#             conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
#         if item_description:
#             item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
#             conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
#         if store_name:
#             store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
#             conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
#         if city:
#             city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
#             conditions.append(SalesAllInOneLive.city.in_(city_list))
#         if demo_flag:
#             demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
#             conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

#         if price_breakup2:
#             sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
#             price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
#             # Create a list to hold the sub-conditions for price ranges            
#             for price_range in price_breakup2_list:
#                 if price_range == '0-1000':
#                     price_conditions.append(sales_per_unit <= 1000)
#                 elif price_range == '1001-2000':
#                     price_conditions.append(sales_per_unit.between(1000, 2000))
#                 elif price_range == '2001-3000':
#                     price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
#                 elif price_range == '3001-4000':
#                     price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
#                 elif price_range == '4001-5000':
#                     price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
#                 elif price_range == '5001-6000':
#                     price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
#                 elif price_range == '6001-7000':
#                     price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
#                 elif price_range == '7001-8000':
#                     price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
#                 elif price_range == '8001-9000':
#                     price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
#                 elif price_range == '9001-10000':
#                     price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
#                 elif price_range == '10001-20000':
#                     price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
#                 elif price_range == '20001-30000':
#                     price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
#                 elif price_range == '30001-40000':
#                     price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
#                 elif price_range == '40001-50000':
#                     price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
#                 elif price_range == '>50000':
#                     price_conditions.append(sales_per_unit > 50000)
#             # Combine all the sub-conditions using OR logic
#         if price_conditions:
#          conditions.append(or_(*price_conditions))
         
#         weekly_sales_query = db.session.query(
#             week_number.label("week_number"),
#             extract("month", SalesAllInOneLive.invoice_date).label("month"),
#             extract("year", SalesAllInOneLive.invoice_date).label("year"),
#             sales_details_field.label("sales_details"),
#         ).filter(*conditions)

#         weekly_sales_query = (
#             weekly_sales_query.group_by(
#                 week_number,
#                 extract("year", SalesAllInOneLive.invoice_date),
#                 extract("month", SalesAllInOneLive.invoice_date),
#             )
#             .order_by(week_number)
#             .offset(offset)
#             .limit(limit)
#         )

#         weekly_sales = weekly_sales_query.all()

#         # Month names and result formatting
#         month_names = {
#             4: "Apr",
#             5: "May",
#             6: "Jun",
#             7: "Jul",
#             8: "Aug",
#             9: "Sep",
#             10: "Oct",
#             11: "Nov",
#             12: "Dec",
#             1: "Jan",
#             2: "Feb",
#             3: "Mar",
#         }

#         result_dict = {}
#         years_list = []
#         yearly_totals = {}

#         for week_number, month, year, sales_details in weekly_sales:
#             if week_number > 52:
#                 continue
            
#             if month <4:
#                 fiscal_year = year
#             else:
#                 fiscal_year = year + 1

#             if fiscal_year not in years_list:
#                 years_list.append(fiscal_year)

#             week_label = f"Week {int(week_number):02}"

#             if week_label not in result_dict:
#                 result_dict[week_label] = {}

#             result_dict[week_label][fiscal_year] = {"sales_details": sales_details}

#             if fiscal_year not in yearly_totals:
#                 yearly_totals[fiscal_year] = 0
#             yearly_totals[fiscal_year] += sales_details

#         for week_label, year_data in result_dict.items():
#             for fiscal_year, data in year_data.items():
#                 yearly_total = yearly_totals.get(fiscal_year, 0)
#                 if yearly_total > 0:
#                     percentage = round((data["sales_details"] / yearly_total) * 100, 2)
#                     result_dict[week_label][fiscal_year] = f"{data['sales_details']} ({percentage}%)"

#         years_list.sort(reverse=True)

#         return jsonify({"values": result_dict, "years": years_list, "page": page, "limit": limit}), 200

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_sales_all_in_one_live_weekly_analysis_cr_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})
# def get_sales_all_in_one_live_weekly_analysis_cr_controller():
#     try:

#         # Determine the factor from request arguments
#         factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

#         # Pagination
#         page = request.args.get("page", 1, type=int)
#         limit = request.args.get("limit", 10, type=int)
#         offset = (page - 1) * limit

#         period_from = request.args.get("period_from")
#         period_to = request.args.get("period_to")
#         invoice_date = request.args.get('invoice_date')
#         srn_flag = request.args.get('srn_flag')
#         sales_type = request.args.get('sales_type')
#         section = request.args.get('section')
#         product_group = request.args.get('product_group')
#         item_category = request.args.get('item_category')
#         brand_name = request.args.get('brand_name')
#         model_no = request.args.get('model_no')
#         item_description = request.args.get('item_description')
#         store_name = request.args.get('store_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         asm = request.args.get('asm')
#         store_code = request.args.get('storecode')

#         # Initialize conditions
#         conditions = []
#         price_conditions = []

#         # Apply dynamic conditions
#         if asm:
#             conditions.append(SalesAllInOneLive.asm == asm)
#         if store_code:
#             store_code_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
#             conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
#         if period_from:
#             conditions.append(SalesAllInOneLive.invoice_date >= period_from)
#         if period_to:
#             conditions.append(SalesAllInOneLive.invoice_date <= period_to)
#         if invoice_date:
#             invoice_date_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
#             conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
#         if srn_flag:
#             srn_flag_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
#             conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
#         if sales_type:
#             sale_types_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
#             conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
#         if section:
#             section_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', section) if isinstance(section, str) else section
#             conditions.append(SalesAllInOneLive.section.in_(section_list))
#         if product_group:
#             product_group_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
#             conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
#         if item_category:
#             item_category_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
#             conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
#         if brand_name:
#             brand_name_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
#             conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
#         if model_no:
#             model_no_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
#             conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
#         if item_description:
#             item_description_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
#             conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
#         if store_name:
#             store_name_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
#             conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
#         if city:
#             city_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', city) if isinstance(city, str) else city
#             conditions.append(SalesAllInOneLive.city.in_(city_list))
#         if demo_flag:
#             demo_flag_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
#             conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

#         if price_breakup2:
#             sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
#             price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
#             # Create a list to hold the sub-conditions for price ranges            
#             for price_range in price_breakup2_list:
#                 if price_range == '0-1000':
#                     price_conditions.append(sales_per_unit <= 1000)
#                 elif price_range == '1001-2000':
#                     price_conditions.append(sales_per_unit.between(1000, 2000))
#                 elif price_range == '2001-3000':
#                     price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
#                 elif price_range == '3001-4000':
#                     price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
#                 elif price_range == '4001-5000':
#                     price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
#                 elif price_range == '5001-6000':
#                     price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
#                 elif price_range == '6001-7000':
#                     price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
#                 elif price_range == '7001-8000':
#                     price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
#                 elif price_range == '8001-9000':
#                     price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
#                 elif price_range == '9001-10000':
#                     price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
#                 elif price_range == '10001-20000':
#                     price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
#                 elif price_range == '20001-30000':
#                     price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
#                 elif price_range == '30001-40000':
#                     price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
#                 elif price_range == '40001-50000':
#                     price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
#                 elif price_range == '>50000':
#                     price_conditions.append(sales_per_unit > 50000)
#             # Combine all the sub-conditions using OR logic
#         if price_conditions:
#          conditions.append(or_(*price_conditions))

#         final_conditions = " AND ".join(
#         str(cond.compile(compile_kwargs={"literal_binds": True})).replace('"', '').replace("sales_all_in_one_live.", "")
#         for cond in conditions
#     ) if conditions else ""
         
#         weekly_sales_query = text(f'''
#                 SELECT 
#     WEEKOFYEAR(invoice_date) AS week_number,
#     MONTH(invoice_date) AS month,
#     CASE
#         WHEN MONTH(invoice_date) >= 4 THEN YEAR(invoice_date)
#         ELSE YEAR(invoice_date) - 1
#     END AS fiscal_year,
#     CONCAT(
#         CASE 
#             WHEN MONTH(invoice_date) >= 4 THEN YEAR(invoice_date)
#             ELSE YEAR(invoice_date) - 1 
#         END,
#         '-',
#         LPAD(WEEKOFYEAR(invoice_date), 2, '0')
#     ) AS fiscal_yweek,
#     SUM(total_sales) AS total_sales,
#     SUM(sales_qty) AS sales_qty,
#     SUM(tax_amt) AS tax_amt,
#     SUM(gros_profit) AS gros_profit
# FROM 
#     apx_stock_apps.sales_all_in_one_live
#                 WHERE 1 = 1
#             {f"AND {final_conditions}" if final_conditions else ""}
# GROUP BY 
#     fiscal_yweek, fiscal_year, week_number, month
# ORDER BY 
#     fiscal_year, month, week_number;
#         ''')
 
#         # Execute the query
#         weekly_sales = db.session.execute(weekly_sales_query).fetchall()

#         # Month mapping
#         month_names = {
#             4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
#             10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar"
#         }

#         result_dict = {}
#         years_list = []
#         yearly_totals = {}

#         for week_number, month, fiscal_year, fiscal_yweek, total_sales, sales_qty, tax_amt, gros_profit in weekly_sales:
#             if month >= 3:
#              fiscal_years = fiscal_year 
#             else:
#              fiscal_years = fiscal_year + 1

#             # Determine sales details based on factor
#             if factor == 'cr':
#                 value = 10_000_000
#                 sales_details = round(total_sales / value, 2)
#             elif factor == 'cr_without_gst':
#                 value = 10_000_000
#                 sales_details = round((total_sales - tax_amt) / value, 2)
#             elif factor == 'lk':
#                 value = 100_000
#                 sales_details = round(total_sales / value, 2)
#             elif factor == 'lk_without_gst':
#                 value = 100_000
#                 sales_details = round((total_sales - tax_amt) / value, 2)
#             elif factor == 'sales_qty':
#                 sales_details = sales_qty
#             elif factor == 'total_sales':
#                 sales_details = total_sales
#             elif factor == 'gp':
#                 value = 100_000
#                 sales_details = round(gros_profit / value, 2)
#             else:
#                 sales_details = total_sales

#             if fiscal_years not in years_list:
#                 years_list.append(fiscal_years)

#             week_label = f"Week {int(week_number):02}"

#             if week_label not in result_dict:
#                 result_dict[week_label] = {}

#             result_dict[week_label][fiscal_years] = {"sales_details": sales_details}

#             if fiscal_years not in yearly_totals:
#                 yearly_totals[fiscal_years] = 0
#             yearly_totals[fiscal_years] += sales_details

#         # Add percentage values
#         for week_label, year_data in result_dict.items():
#             for fiscal_years, data in year_data.items():
#                 yearly_total = yearly_totals.get(fiscal_years, 0)
#                 if yearly_total > 0:
#                     percentage = round((data["sales_details"] / yearly_total) * 100, 2)
#                     result_dict[week_label][fiscal_years] = f"{data['sales_details']} ({percentage}%)"

#         years_list.sort(reverse=True)

#         return jsonify({"values": result_dict, "years": years_list}), 200

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_sales_all_in_one_live_weekly_analysis_cr_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_weekly_analysis_cr_controller():
    try:

        # Determine the factor from request arguments
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        final_conditions = " AND ".join(
        str(cond.compile(compile_kwargs={"literal_binds": True})).replace('"', '').replace("sales_all_in_one_live.", "")
        for cond in conditions
    ) if conditions else ""
         
        weekly_sales_query = text(f'''
WITH fiscal_start_date AS (  
    SELECT  
        invoice_date,  
        total_sales,  
        SALES_QTY,  
        tax_amt,  
        gros_profit,  
        -- Adjust fiscal year (April to March)
        CASE  
            WHEN MONTH(invoice_date) >= 4 THEN YEAR(invoice_date) + 1  
            ELSE YEAR(invoice_date)  
        END AS fiscal_year  
    FROM apx_stock_apps.sales_all_in_one_live  
       WHERE 1 = 1
            {f"AND {final_conditions}" if final_conditions else ""}   
),  
week_calculations AS (  
    SELECT  
        fiscal_year,  
        -- Get the week number starting from April as Week 1
        DENSE_RANK() OVER (PARTITION BY fiscal_year ORDER BY MIN(invoice_date)) AS fiscal_week_number,  
        MIN(invoice_date) AS week_start,  
        MAX(invoice_date) AS week_end,  
        MONTH(MIN(invoice_date)) AS month,  
        SUM(total_sales) AS totalsales,  
        SUM(SALES_QTY) AS salesqty,  
        SUM(tax_amt) AS taxamt,  
        SUM(gros_profit) AS grosprofit  
    FROM fiscal_start_date  
    GROUP BY fiscal_year, YEARWEEK(invoice_date, 7)  -- Group by Saturday-Sunday weeks  
)  
SELECT  
    fiscal_week_number AS week_number,  
    week_start,  
    week_end,  
    month,  
    fiscal_year,  
    totalsales,  
    salesqty,  
    taxamt,  
    grosprofit  
FROM week_calculations  
ORDER BY fiscal_year, fiscal_week_number;
        ''')

        # Execute the query with conditions, offset, and limit
        weekly_sales = db.session.execute(weekly_sales_query).fetchall()


            # Month names and result formatting
        month_names = {
                4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
                10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar"
            }

        result_dict = {}
        years_list = []
        yearly_totals = {}
        fiscal_years=0000

        for week_number,week_start, month,week_end, fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in weekly_sales:

                fiscal_years=fiscal_year

                # Calculate sales_details based on the factor
                if factor == 'cr':
                    value = 10000000
                    sales_details = round(total_sales / value, 2)
                elif factor == 'cr_without_gst':
                    value = 10000000
                    sales_details = round((total_sales - tax_amt) / value, 2)
                elif factor == 'lk':
                    value = 100000
                    sales_details = round(total_sales / value, 2)
                elif factor == 'lk_without_gst':
                    value = 100000
                    sales_details = round((total_sales - tax_amt) / value, 2)
                elif factor == 'sales_qty':
                    sales_details = sales_qty
                elif factor == 'total_sales':
                    sales_details = total_sales
                elif factor == 'gp':
                    value = 100000
                    sales_details = round(gros_profit / value, 2)
                else:
                    sales_details = total_sales

                if fiscal_years not in years_list:
                    years_list.append(fiscal_years)

                week_label = f"Week {int(week_number):02}"

                if week_label not in result_dict:
                    result_dict[week_label] = {}

                result_dict[week_label][fiscal_years] = {"sales_details": sales_details}

                if fiscal_years not in yearly_totals:
                    yearly_totals[fiscal_years] = 0
                yearly_totals[fiscal_years] += sales_details

        for week_label, year_data in result_dict.items():
                for fiscal_years, data in year_data.items():
                    yearly_total = yearly_totals.get(fiscal_years, 0)
                    if yearly_total > 0:
                        percentage = round((data["sales_details"] / yearly_total) * 100, 2)
                        result_dict[week_label][fiscal_years] = f"{data['sales_details']} ({percentage}%)"
        
        # Convert keys to a list for pagination
        week_labels = list(result_dict.keys())
        paginated_week_labels = week_labels[offset:offset + limit]

        # Construct a new paginated dictionary
        paginated_dict = {week_label: result_dict[week_label] for week_label in paginated_week_labels}

        # Process data safely
        for week_label, year_data in paginated_dict.items():
            if not isinstance(year_data, dict):  # Ensure it's a dictionary
                continue

            for fiscal_years, data in year_data.items():
                if not isinstance(data, dict):  # Ensure data is a dictionary
                    continue
                
                yearly_total = yearly_totals.get(fiscal_years, 0)
                if yearly_total > 0 and "sales_details" in data:
                    percentage = round((data["sales_details"] / yearly_total) * 100, 2)
                    paginated_dict[week_label][fiscal_years] = f"{data['sales_details']} ({percentage}%)"



        years_list.sort(reverse=True)

        return jsonify({"values": paginated_dict, "years": years_list}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_weekly_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# ----------------------------------------- Day Analysis -------------------------------------

def get_sales_all_in_one_live_day_analysis_cr_controller():
    try:
        # Retrieve the factor parameter with a default value of 'cr'
        factor = request.args.get('gstfilter', 'cr') 
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Pagination parameters
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        # Step 1: Retrieve distinct years
        years_query = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).distinct().label("year")
        ).order_by(extract("year", SalesAllInOneLive.invoice_date).asc())

        # Fetch the list of distinct years
        distinct_years = [row.year for row in years_query.all()]
        distinct_years.reverse()
        # print(distinct_years)
        # Step 2: Determine the year based on the page number
        if page <= len(distinct_years):
            selected_year = distinct_years[page - 1]  # Page 1 gets the first year, Page 2 gets the second, and so on.

            # Step 3: Filter data by the selected year
        month_order = {
            1:"January", 2:"February",  3:"March", 4:"April", 5:"May", 6:"June",
            7:"July", 8:"August", 9:"September", 10:"October", 
            11:"November",12:"December"
        }
        
        fiscal_start = case(
        (func.month(SalesAllInOneLive.invoice_date) >= 4,
         func.concat(func.year(SalesAllInOneLive.invoice_date), '-04-01')),
        else_=func.concat(func.year(SalesAllInOneLive.invoice_date) - 1, '-04-01')
    )

    # Compute fiscal week as the number of weeks since the fiscal start date.
    # This calculates the difference in days, divides by 7, floors the result, and adds 1.
        fiscal_week = (func.floor(
                        func.datediff(SalesAllInOneLive.invoice_date, fiscal_start) / 7
                    ) + 1).label("fiscal_week")

    # Build the query with additional computed columns for fiscal_month, fiscal_day, and fiscal_week.
        sales_data_query = db.session.query(
            func.date_format(SalesAllInOneLive.invoice_date, "%M").label("month"),  # Month name
            extract("year", SalesAllInOneLive.invoice_date).label("year"),           # Calendar year
            # Fiscal year: if invoice month is Jan-Mar, fiscal year is same as invoice year,
            # else add 1 to the invoice year (i.e. April to Dec belong to the next fiscal year).
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4,
                func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.month(SalesAllInOneLive.invoice_date).label("fiscal_month"),
            func.day(SalesAllInOneLive.invoice_date).label("fiscal_day"),
            fiscal_week,  # Custom computed fiscal week
            func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(
            *conditions
        )

        # Group by fiscal_year, fiscal_week, and day_of_week.
        sales_data_query = sales_data_query.group_by(
            "fiscal_year",
            "fiscal_week",
            func.dayofweek(SalesAllInOneLive.invoice_date)
        )

        sales_data = sales_data_query.all()

        # Initialize containers and a day mapping dictionary.
        formatted_data = []
        formatted_data1 = []

        # day_mapping = {
        #     1: "mon",
        #     2: "tue",
        #     3: "wed",
        #     4: "thu",
        #     5: "fri",
        #     6: "sat",
        #     7: "sun",
        # }

        day_mapping = {
            2: "mon",
            3: "tue",
            4: "wed",
            5: "thu",
            6: "fri",
            7: "sat",
            1: "sun",
        }

        # Nested dictionary structure: fiscal_year -> fiscal_week -> (a dict for the week details)
        grouped_sales = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "year": None,
                    "fiscal_month": None,  # you can use one representative month for display
                    "fiscal_day": None,    # same for day if needed
                    "fiscal_week": None,
                    "mon": {"sales_details": "-", "percentage": "-"},
                    "tue": {"sales_details": "-", "percentage": "-"},
                    "wed": {"sales_details": "-", "percentage": "-"},
                    "thu": {"sales_details": "-", "percentage": "-"},
                    "fri": {"sales_details": "-", "percentage": "-"},
                    "sat": {"sales_details": "-", "percentage": "-"},
                    "sun": {"sales_details": "-", "percentage": "-"},
                }
            )
        )

        # Store weekly totals for percentage calculation.
        weekly_totals = defaultdict(lambda: defaultdict(float))

        # Loop over each row in the query result.
        for month, year, fiscal_year, fiscal_month, fiscal_day, fiscal_week, day_of_week, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            # Calculate sales_details based on the provided factor.
            if factor == 'cr':
                value = 10000000 
                sales_details = round(total_sales / value, 2)
            elif factor == 'cr_without_gst':
                value = 10000000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'lk':
                value = 100000 
                sales_details = round(total_sales / value, 2)
            elif factor == 'lk_without_gst':
                value = 100000 
                sales_details = round((total_sales - tax_amt) / value, 2)
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                value = 100000 
                sales_details = round(gros_profit / value, 2)
            else:
                sales_details = total_sales

            day_key = day_mapping.get(day_of_week, "unknown")
            week_key = f"Week{int(fiscal_week)}"

            # Populate the grouped_sales dictionary.
            grouped_sales[fiscal_year][week_key][day_key]["sales_details"] = sales_details

            # Sum up the sales for the week for percentage calculations.
            weekly_totals[fiscal_year][week_key] += float(sales_details)

            # Optionally, you might wish to store one representative fiscal_month or fiscal_day for display.
            grouped_sales[fiscal_year][week_key]["month"] = month_order.get(fiscal_month, "-") 
            grouped_sales[fiscal_year][week_key]["fiscal_day"] = fiscal_day
            grouped_sales[fiscal_year][week_key]["week"] = week_key
            grouped_sales[fiscal_year][week_key]["weeks"] = fiscal_week

        # Process each grouped week to calculate percentages.
        for fiscal_year, weeks in grouped_sales.items():
            for week, sales in weeks.items():
                weekly_total = weekly_totals[fiscal_year][week]
                for day in day_mapping.values():
                    if weekly_total > 0:
                        # Check if there is a sales value before calculating percentage.
                        if sales[day]["sales_details"] != "-":
                            percentage = round((float(sales[day]["sales_details"]) / weekly_total) * 100, 2)
                            # Format as "value (percentage%)"
                            sales[day] = f"{sales[day]['sales_details']} ({percentage}%)"
                        else:
                            sales[day] = f"{0.00} ({0.00}%)"
                    else:
                        sales[day] = f"{0.00} ({0.00}%)"

                # Store additional fiscal information for the week.
                sales["year"] = str(fiscal_year)
                sales["fiscal_week"] = week
                sales["fiscal_month"] = sales.get("fiscal_month", "-")  # representative month if needed

                formatted_data.append(sales)
                current_month = datetime.now().strftime("%B")
                formatted_data.sort(
                        key=lambda x: (
                            x["year"],
                            x["month"] == current_month,
                            month_index(x["month"]),
                            int(x["weeks"])
                        )
                    )
                formatted_data.reverse()
                # current_month = datetime.now().strftime("%B")

                # # Filter data for the current month
                # filtered_data = [item for item in formatted_data if item["month"] == current_month]

                # # Sort the filtered data by week number
                # filtered_data.sort(key=lambda x: int(x["weeks"]))
                # filtered_data.reverse()

        return jsonify(formatted_data), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_day_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})
# Function to get the index of the month in reordered_months
def month_index(month_name):
    month_order = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Get the current month and rearrange the month order
    current_month = datetime.now().strftime("%B")
    current_month_index = month_order.index(current_month)
    # print(current_month_index+1)
    # print(current_month)
    reordered_months = month_order[current_month_index:] + month_order[:current_month_index]
    return reordered_months.index(month_name) if month_name in reordered_months else -1
# --------------------------------------- Product Dimension ----------------------------------

# def get_sales_all_in_one_live_product_dimension_cr_controller():
#     try:

          
#         period_from = request.args.get("period_from")
#         period_to = request.args.get("period_to")
#         invoice_date = request.args.get('invoice_date')
#         srn_flag = request.args.get('srn_flag')
#         sales_type = request.args.get('sales_type')
#         section = request.args.get('section')
#         product_group = request.args.get('product_group')
#         item_category = request.args.get('item_category')
#         brand_name = request.args.get('brand_name')
#         model_no = request.args.get('model_no')
#         item_description = request.args.get('item_description')
#         store_name = request.args.get('store_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         asm = request.args.get('asm')
#         store_code = request.args.get('storecode')

#         # Initialize conditions
#         conditions = []
#         price_conditions = []

#         # Apply dynamic conditions
#         if asm:
#             conditions.append(SalesAllInOneLive.asm == asm)
#         if store_code:
#             store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
#             conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
#         if period_from:
#             conditions.append(SalesAllInOneLive.invoice_date >= period_from)
#         if period_to:
#             conditions.append(SalesAllInOneLive.invoice_date <= period_to)
#         if invoice_date:
#             invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
#             conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
#         if srn_flag:
#             srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
#             conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
#         if sales_type:
#             sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
#             conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
#         if section:
#             section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
#             conditions.append(SalesAllInOneLive.section.in_(section_list))
#         if product_group:
#             product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
#             conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
#         if item_category:
#             item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
#             conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
#         if brand_name:
#             brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
#             conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
#         if model_no:
#             model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
#             conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
#         if item_description:
#             item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
#             conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
#         if store_name:
#             store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
#             conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
#         if city:
#             city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
#             conditions.append(SalesAllInOneLive.city.in_(city_list))
#         if demo_flag:
#             demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
#             conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


#         if price_breakup2:
#             sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
#             price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
#             # Create a list to hold the sub-conditions for price ranges            
#             for price_range in price_breakup2_list:
#                 if price_range == '0-1000':
#                     price_conditions.append(sales_per_unit <= 1000)
#                 elif price_range == '1001-2000':
#                     price_conditions.append(sales_per_unit.between(1000, 2000))
#                 elif price_range == '2001-3000':
#                     price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
#                 elif price_range == '3001-4000':
#                     price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
#                 elif price_range == '4001-5000':
#                     price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
#                 elif price_range == '5001-6000':
#                     price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
#                 elif price_range == '6001-7000':
#                     price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
#                 elif price_range == '7001-8000':
#                     price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
#                 elif price_range == '8001-9000':
#                     price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
#                 elif price_range == '9001-10000':
#                     price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
#                 elif price_range == '10001-20000':
#                     price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
#                 elif price_range == '20001-30000':
#                     price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
#                 elif price_range == '30001-40000':
#                     price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
#                 elif price_range == '40001-50000':
#                     price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
#                 elif price_range == '>50000':
#                     price_conditions.append(sales_per_unit > 50000)
#             # Combine all the sub-conditions using OR logic
#             if price_conditions:
#              conditions.append(or_(*price_conditions))
#         # Get request parameters
#         factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'
#         page = request.args.get("page", 1, type=int)
#         limit = request.args.get("limit", 10, type=int)
#         offset = (page - 1) * limit
        
#         # Set factor value
#         if factor == 'cr':
#             value = 10000000
#         elif factor == 'cr_without_gst':
#             value = 10000000
#         elif factor == 'lk':
#             value = 100000
#         elif factor == 'lk_without_gst':
#             value = 100000
#         elif factor == 'sales_qty':
#             value = None  # Handle separately
#         elif factor == 'total_sales':
#             value = None  # Handle separately
#         elif factor == 'gp':
#             value = 100000
#         else:
#             return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

#         # Query sales data with pagination
#         sales_data = db.session.query(
#             SalesAllInOneLive.product_group,
#             extract("year", SalesAllInOneLive.invoice_date).label("year"),
#             extract("month", SalesAllInOneLive.invoice_date).label("month"),
#             case(
#                 (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
#                 else_=func.year(SalesAllInOneLive.invoice_date)
#             ).label("fiscal_year"),
#             func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
#             func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
#             func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
#             func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
#         ).filter(*conditions)

#         # Apply grouping and ordering
#         sales_data = sales_data.group_by(
#             SalesAllInOneLive.product_group,
#             "fiscal_year",
#             extract("month", SalesAllInOneLive.invoice_date)
#         ).order_by(SalesAllInOneLive.product_group).limit(limit).offset(offset).all()


#         sales_datas1 = db.session.query(
#             SalesAllInOneLive.product_group,
#             extract("year", SalesAllInOneLive.invoice_date).label("year"),
#             extract("month", SalesAllInOneLive.invoice_date).label("month"),
#             case(
#                 (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
#                 else_=func.year(SalesAllInOneLive.invoice_date)
#             ).label("fiscal_year"),
#             func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
#             func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
#             func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
#             func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
#         ).filter(*conditions)

#         # Apply grouping and ordering
#         sales_datas1 = sales_datas1.group_by(
#             SalesAllInOneLive.product_group,
#             "fiscal_year",
#             extract("month", SalesAllInOneLive.invoice_date)
#         ).order_by(SalesAllInOneLive.product_group)
#         sales_data1 = sales_datas1.all()
#         # Process sales data and calculate yearly totals and percentages
#         result_dict = {}
#         month_names = {
#             4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
#             10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar",
#         }
#         month_names1 = {
#             "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
#             "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
#         }

#         years_list = []
#         yearly_totals = {}

#         for product_group, year, month, fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
#             financial_month = month_names[month]

#             if fiscal_year not in years_list:
#                 years_list.append(fiscal_year)

#             # Set default values
#             if total_sales is None:
#                 total_sales = 0
#                 sales_qty = 0
#                 tax_amt = 0
#                 gros_profit = 0

#             if gros_profit is None:
#                 gros_profit = 0
#             if total_sales is None:
#                 total_sales = 0
#             if tax_amt is None:
#                 tax_amt = 0

#             # Calculate sales details based on the selected factor
#             if value is None or value == 0:
#                if factor == 'sales_qty':
#                    sales_details = sales_qty
#                elif factor == 'total_sales':
#                    sales_details = total_sales
#             elif factor == 'sales_qty':
#                 sales_details = sales_qty
#             elif factor == 'total_sales':
#                 sales_details = total_sales
#             elif factor == 'gp':
#                 sales_details = round(gros_profit / value, 2)
#             else:
#                 if factor in ['cr', 'cr_without_gst']:
#                     if factor == 'cr_without_gst':
#                         sales_details = round((total_sales - tax_amt) / value, 2)
#                     else:
#                         sales_details = round(total_sales / value, 2)
#                 elif factor in ['lk', 'lk_without_gst']:
#                     if factor == 'lk_without_gst':
#                         sales_details = round((total_sales - tax_amt) / value, 2)
#                     else:
#                         sales_details = round(total_sales / value, 2)
            
#             sales_with_gst = sales_details
         
#             if product_group not in result_dict:
#                 result_dict[product_group] = {}

#             if fiscal_year not in result_dict[product_group]:
#                 result_dict[product_group][fiscal_year] = {}

#             if product_group not in yearly_totals:
#                 yearly_totals[product_group] = {}

#             if fiscal_year not in yearly_totals[product_group]:
#                 yearly_totals[product_group][fiscal_year] = 0
            
#             yearly_totals[product_group][fiscal_year] = Decimal(yearly_totals[product_group][fiscal_year])
#             yearly_totals[product_group][fiscal_year] += Decimal(sales_with_gst)
#             result_dict[product_group][fiscal_year][financial_month] = {"sales_with_gst": sales_with_gst}
        
#         monthly_totals = {}
#         for product_group, year, month, fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data1:

#             if total_sales is None:
#                 total_sales = 0
#             if sales_qty is None:
#                 sales_qty = 0
#             if tax_amt is None:
#                 tax_amt = 0
#             if gros_profit is None:
#                 gros_profit = 0

#             # Determine value based on gstfilter
#             if factor == 'cr':
#                 value = 10000000
#             elif factor == 'cr_without_gst':
#                 value = 10000000
#             elif factor == 'lk':
#                 value = 100000
#             elif factor == 'lk_without_gst':
#                 value = 100000
#             elif factor == 'sales_qty':
#                 value = None  # Handle separately, as sales_qty doesn't need a factor
#             elif factor == 'total_sales':
#                 value = None  # Handle separately
#             elif factor == 'gp':
#                 value = 100000
#             else:
#                 return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

#             # Calculate sales details based on the selected factor
#             if factor == 'sales_qty':
#                     sales_details = sales_qty
#             elif factor == 'total_sales':
#                     sales_details = total_sales
#             elif factor == 'gp':
#                     sales_details = gros_profit / value
#             else:
#                     if factor in ['cr', 'cr_without_gst']:
#                         if factor == 'cr_without_gst':
#                             sales_details = (total_sales - tax_amt) / value
#                         else:
#                             sales_details = total_sales / value
#                     elif factor in ['lk', 'lk_without_gst']:
#                         if factor == 'lk_without_gst':
#                             sales_details = (total_sales - tax_amt) / value
#                         else:
#                             sales_details = total_sales / value

#             sales_with_gst = Decimal(sales_details)

#             if fiscal_year not in monthly_totals:
#                     monthly_totals[fiscal_year] = {}
#             if month not in monthly_totals[fiscal_year]:
#                     monthly_totals[fiscal_year][month] = Decimal(0)

#                 # Add up sales for each month with full precision
#             monthly_totals[fiscal_year][month] += sales_with_gst

#             # Retrieve the specific monthly total
#         # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
#         # print(monthly_total1)

#         # monthly_totals = {}
#         # for product_group, fiscal_year_data in result_dict.items():
#         #     for fiscal_year, months_data in fiscal_year_data.items():
#         #         for month, data in months_data.items():
                    
#         #             if fiscal_year not in monthly_totals:
#         #                 monthly_totals[fiscal_year] = {}
                    
#         #             if month not in monthly_totals[fiscal_year]:
#         #                 monthly_totals[fiscal_year][month] = 0

#         #             # Add up sales for each month
#         #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
#         #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

#         # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
#         for product_group, fiscal_year_data in result_dict.items():
#             for fiscal_year, months_data in fiscal_year_data.items():
#                 for month, data in months_data.items():
#                     financial_month = month_names1[month]
#                     monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

#                     sales_with_gst = round(Decimal(data['sales_with_gst']),2)
#                     if monthly_total == 0:
#                         result_dict[product_group][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
#                     else:
#                         percentage = round((sales_with_gst / monthly_total) * 100, 2)
#                         result_dict[product_group][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"

#         years_list.reverse()
        
#         return jsonify({"years": years_list, "values": result_dict,
#             "yearly_totals": yearly_totals}), 200

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_sales_all_in_one_live_product_dimension_cr_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})
def get_sales_all_in_one_live_product_dimension_cr_controller():
    try:

          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list = store_code.split(',') if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list = invoice_date.split(',') if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list = srn_flag.split(',') if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = sales_type.split(',') if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list = section.split(',') if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list = product_group.split(',') if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list = item_category.split(',') if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list = brand_name.split(',') if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list = model_no.split(',') if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list = item_description.split(',') if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list = store_name.split(',') if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list = city.split(',') if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list = demo_flag.split(',') if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            if price_breakup2 == '0-1000':
                conditions.append(sales_per_unit <= 1000)
            elif price_breakup2 == '1001-2000':
                conditions.append(sales_per_unit.between(1000, 2000))
            elif price_breakup2 == '2001-3000':
                conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
            elif price_breakup2 == '3001-4000':
                conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
            elif price_breakup2 == '4001-5000':
                conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
            elif price_breakup2 == '5001-6000':
                conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
            elif price_breakup2 == '6001-7000':
                conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
            elif price_breakup2 == '7001-8000':
                conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
            elif price_breakup2 == '8001-9000':
                conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
            elif price_breakup2 == '9001-10000':
                conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
            elif price_breakup2 == '10001-20000':
                conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
            elif price_breakup2 == '20001-30000':
                conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
            elif price_breakup2 == '30001-40000':
                conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
            elif price_breakup2 == '40001-50000':
                conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
            elif price_breakup2 == '>50000':
                conditions.append(sales_per_unit > 50000)

        # Get request parameters
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        # Set factor value
        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        # Query sales data with pagination
        sales_data = db.session.query(
            SalesAllInOneLive.product_group,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
        ).filter(*conditions)

        # Apply ordering and pagination
        sales_data = sales_data.group_by(
            SalesAllInOneLive.product_group,
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.product_group).order_by(
            func.sum(SalesAllInOneLive.total_sales).desc()  # Order by value in descending order
        ).all()

        # Process sales data and calculate yearly totals and percentages
        result_dict = {}
        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
            10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar",
        }

        years_list = []
        yearly_totals = {}

        for product_group, year, month, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            fiscal_year = year if month in [1, 2, 3] else year + 1
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            # Set default values
            if total_sales is None:
                total_sales = 0
                sales_qty = 0
                tax_amt = 0
                gros_profit = 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            if product_group not in result_dict:
                result_dict[product_group] = {}

            if fiscal_year not in result_dict[product_group]:
                result_dict[product_group][fiscal_year] = {}

            if product_group not in yearly_totals:
                yearly_totals[product_group] = {}

            if fiscal_year not in yearly_totals[product_group]:
                yearly_totals[product_group][fiscal_year] = 0

            yearly_totals[product_group][fiscal_year] += sales_with_gst
            result_dict[product_group][fiscal_year][financial_month] = {"sales_with_gst": sales_with_gst}

        for product_group, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[product_group][fiscal_year]
                for month, data in months_data.items():
                    if data["sales_with_gst"] == 0:
                        result_dict[product_group][fiscal_year][month] = f"{data['sales_with_gst']} ({0.00}%)"
                    else:
                        percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
                        result_dict[product_group][fiscal_year][month] = f"{data['sales_with_gst']} ({percentage}%)"


        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_product_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# ------------------------------------- Brand Dimension --------------------------------------

def safe_divide(numerator, denominator):
    try:
        return numerator / denominator if denominator != 0 else 0
    except ZeroDivisionError:
        return 0

def get_sales_all_in_one_live_brand_dimension_cr_controller():
    try:
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        gstfilter = request.args.get('gstfilter', 'cr')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                # Add the other ranges as needed
            # Combine all the sub-conditions using OR logic
        if price_conditions:
            conditions.append(or_(*price_conditions))

        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        sales_data_query = db.session.query(
            SalesAllInOneLive.brand_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
        ).filter(*conditions)

        # Apply ordering and pagination
        sales_data_query = sales_data_query.group_by(
            SalesAllInOneLive.brand_name,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.brand_name).limit(limit).offset(offset)

        sales_data = sales_data_query.all()

        sales_data_query1 = db.session.query(
            SalesAllInOneLive.brand_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
        ).filter(*conditions)

        # Apply ordering and pagination
        sales_data_query1 = sales_data_query1.group_by(
            SalesAllInOneLive.brand_name,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.brand_name).limit(limit).offset(offset)

        sales_data1 = sales_data_query1.all()

        result_dict = {}
        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
            10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar",
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }

        years_list = []
        yearly_totals = {}

        for brand_name, year, month, fiscal_year, sales_qty, tax_amt, gros_profit, total_sales in sales_data:
            if brand_name is None or brand_name == "":
                brand_name = "Emp"  # Handle empty brand_name
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            # Set default values if None
            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if gstfilter == 'cr':
                value = 10000000
            elif gstfilter == 'cr_without_gst':
                value = 10000000
            elif gstfilter == 'lk':
                value = 100000
            elif gstfilter == 'lk_without_gst':
                value = 100000
            elif gstfilter == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif gstfilter == 'total_sales':
                value = None  # Handle separately
            elif gstfilter == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {gstfilter}"}), 400

            # Calculate sales details based on the selected factor
            if gstfilter == 'sales_qty':
                sales_details = sales_qty
            elif gstfilter == 'total_sales':
                sales_details = total_sales
            elif gstfilter == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if gstfilter in ['cr', 'cr_without_gst']:
                    if gstfilter == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif gstfilter in ['lk', 'lk_without_gst']:
                    if gstfilter == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details
            if brand_name not in result_dict:
                result_dict[brand_name] = {}

            if fiscal_year not in result_dict[brand_name]:
                result_dict[brand_name][fiscal_year] = {}

            if brand_name not in yearly_totals:
                yearly_totals[brand_name] = {}

            if fiscal_year not in yearly_totals[brand_name]:
                yearly_totals[brand_name][fiscal_year] = 0

            yearly_totals[brand_name][fiscal_year] += sales_with_gst

            result_dict[brand_name][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        monthly_totals = {}
        for brand_name, year, month, fiscal_year, sales_qty, tax_amt, gros_profit, total_sales in sales_data1:

            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if gstfilter == 'cr':
                value = 10000000
            elif gstfilter == 'cr_without_gst':
                value = 10000000
            elif gstfilter == 'lk':
                value = 100000
            elif gstfilter == 'lk_without_gst':
                value = 100000
            elif gstfilter == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif gstfilter == 'total_sales':
                value = None  # Handle separately
            elif gstfilter == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {gstfilter}"}), 400

            # Calculate sales details based on the selected factor
            if gstfilter == 'sales_qty':
                    sales_details = sales_qty
            elif gstfilter == 'total_sales':
                    sales_details = total_sales
            elif gstfilter == 'gp':
                    sales_details = gros_profit / value
            else:
                    if gstfilter in ['cr', 'cr_without_gst']:
                        if gstfilter == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif gstfilter in ['lk', 'lk_without_gst']:
                        if gstfilter == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)
        # max_sales_with_gst = 0
        # for brand_name, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = yearly_totals[brand_name][fiscal_year]
        #         for month, data in months_data.items():
        #             data_sales_with_gst = data["sales_with_gst"]

        #             if data_sales_with_gst > max_sales_with_gst:
        #                 max_sales_with_gst = data_sales_with_gst

        #             if data_sales_with_gst == 0:
        #                 result_dict[brand_name][fiscal_year][month] = f"0 (0.00%)"
        #             else:
        #                 percentage = safe_divide(data_sales_with_gst, yearly_total) * 100
        #                 percentage = round(percentage, 2)
        #                 result_dict[brand_name][fiscal_year][
        #                     month
        #                 ] = f"{data_sales_with_gst} ({percentage}%)"
        # monthly_totals = {}
        # for brand_name, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         for month, data in months_data.items():
        #             if fiscal_year not in monthly_totals:
        #                 monthly_totals[fiscal_year] = {}
                    
        #             if month not in monthly_totals[fiscal_year]:
        #                 monthly_totals[fiscal_year][month] = 0

        #             # Add up sales for each month
        #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
        #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
        for brand_name, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[brand_name][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[brand_name][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict,
            "yearly_totals": yearly_totals}), 200

    except Exception as e:
        db.session.rollback()
        # Improved error handling to include traceback for debugging
        # print("Error:", str(e))
        # print(traceback.format_exc())
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)}), 500


# -------------------------------------- Item Dimension --------------------------------------

def safe_divide(numerator, denominator):
    """Safely divide two numbers, returning 0 if the denominator is zero."""
    return numerator / denominator if denominator != 0 else 0

def get_sales_all_in_one_live_item_dimension_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        gstfilter = request.args.get('gstfilter','cr')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges
    
            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        sales_data_query = db.session.query(
            SalesAllInOneLive.item_description,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),  # Assuming this field exists
        ).filter(*conditions)

        # Apply ordering and pagination
        sales_data_query = sales_data_query.group_by(
            SalesAllInOneLive.item_description,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.item_description).limit(limit).offset(offset)
        sales_data_query1 = db.session.query(
            SalesAllInOneLive.item_description,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),  # Assuming this field exists
        ).filter(*conditions)

        # Apply ordering and pagination
        sales_data_query1 = sales_data_query1.group_by(
            SalesAllInOneLive.item_description,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.item_description)
        sales_data1 = sales_data_query1.all()
        sales_data = sales_data_query.all()

        result_dict = {}
        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
            10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar",
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }

        years_list = []
        yearly_totals = {}

        for item_description, year, month, fiscal_year, sales_qty, tax_amt, gros_profit, total_sales in sales_data:
            if item_description is None or item_description == "":
                item_description = "Emp"  # Handle empty item_description
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            # Set default values if None
            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if gstfilter == 'cr':
                value = 10000000
            elif gstfilter == 'cr_without_gst':
                value = 10000000
            elif gstfilter == 'lk':
                value = 100000
            elif gstfilter == 'lk_without_gst':
                value = 100000
            elif gstfilter == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif gstfilter == 'total_sales':
                value = None  # Handle separately
            elif gstfilter == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {gstfilter}"}), 400

            # Calculate sales details based on the selected factor
            if gstfilter == 'sales_qty':
                sales_details = sales_qty
            elif gstfilter == 'total_sales':
                sales_details = total_sales
            elif gstfilter == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if gstfilter in ['cr', 'cr_without_gst']:
                    if gstfilter == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif gstfilter in ['lk', 'lk_without_gst']:
                    if gstfilter == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details
            if item_description not in result_dict:
                result_dict[item_description] = {}

            if fiscal_year not in result_dict[item_description]:
                result_dict[item_description][fiscal_year] = {}

            if item_description not in yearly_totals:
                yearly_totals[item_description] = {}

            if fiscal_year not in yearly_totals[item_description]:
                yearly_totals[item_description][fiscal_year] = 0

            yearly_totals[item_description][fiscal_year] += sales_with_gst

            result_dict[item_description][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }
        
        monthly_totals = {}
        for item_description, year, month, fiscal_year, sales_qty, tax_amt, gros_profit, total_sales in sales_data1:
            

            
            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if gstfilter == 'cr':
                value = 10000000
            elif gstfilter == 'cr_without_gst':
                value = 10000000
            elif gstfilter == 'lk':
                value = 100000
            elif gstfilter == 'lk_without_gst':
                value = 100000
            elif gstfilter == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif gstfilter == 'total_sales':
                value = None  # Handle separately
            elif gstfilter == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {gstfilter}"}), 400

            # Calculate sales details based on the selected factor
            if gstfilter == 'sales_qty':
                    sales_details = sales_qty
            elif gstfilter == 'total_sales':
                    sales_details = total_sales
            elif gstfilter == 'gp':
                    sales_details = gros_profit / value
            else:
                    if gstfilter in ['cr', 'cr_without_gst']:
                        if gstfilter == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif gstfilter in ['lk', 'lk_without_gst']:
                        if gstfilter == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)
        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month

        for item_description, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[item_description][fiscal_year]
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[item_description][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[item_description][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"
        # max_sales_with_gst = 0
        # for actual_item, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = yearly_totals[actual_item][fiscal_year]
        #         for month, data in months_data.items():
        #             data_sales_with_gst = data["sales_with_gst"]

        #             if data_sales_with_gst > max_sales_with_gst:
        #                 max_sales_with_gst = data_sales_with_gst

        #             if data_sales_with_gst == 0:
        #                 result_dict[actual_item][fiscal_year][month] = f"0 (0.00%)"
        #             else:
        #                 percentage = safe_divide(data_sales_with_gst, yearly_total) * 100
        #                 percentage = round(percentage, 2)
        #                 result_dict[actual_item][fiscal_year][
        #                     month
        #                 ] = f"{data_sales_with_gst} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict,
            "yearly_totals": yearly_totals}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# --------------------------------------- Price Breakup 1 ------------------------------------

# def get_sales_all_in_one_live_price_breakup_one_cr_controller():
#     try:
          
#         period_from = request.args.get("period_from")
#         period_to = request.args.get("period_to")
#         invoice_date = request.args.get('invoice_date')
#         srn_flag = request.args.get('srn_flag')
#         sales_type = request.args.get('sales_type')
#         section = request.args.get('section')
#         product_group = request.args.get('product_group')
#         item_category = request.args.get('item_category')
#         brand_name = request.args.get('brand_name')
#         model_no = request.args.get('model_no')
#         item_description = request.args.get('item_description')
#         store_name = request.args.get('store_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         asm = request.args.get('asm')
#         store_code = request.args.get('storecode')

#         # Initialize conditions
#         conditions = []

#         # Apply dynamic conditions
#         if asm:
#             conditions.append(SalesAllInOneLive.asm == asm)
#         if store_code:
#             conditions.append(SalesAllInOneLive.store_code == store_code)
#         if period_from:
#             conditions.append(SalesAllInOneLive.invoice_date >= period_from)
#         if period_to:
#             conditions.append(SalesAllInOneLive.invoice_date <= period_to)
#         if invoice_date:
#             conditions.append(SalesAllInOneLive.invoice_date == invoice_date)
#         if srn_flag:
#             conditions.append(SalesAllInOneLive.srn_flag == srn_flag)
#         if sales_type:
#             conditions.append(SalesAllInOneLive.sale_type == sales_type)
#         if section:
#             conditions.append(SalesAllInOneLive.section == section)
#         if product_group:
#             conditions.append(SalesAllInOneLive.product_group == product_group)
#         if item_category:
#             conditions.append(SalesAllInOneLive.item_category == item_category)
#         if brand_name:
#             conditions.append(SalesAllInOneLive.brand_name == brand_name)
#         if model_no:
#             conditions.append(SalesAllInOneLive.model_no == model_no)
#         if item_description:
#             conditions.append(SalesAllInOneLive.item_description == item_description)
#         if store_name:
#             conditions.append(SalesAllInOneLive.store_name == store_name)
#         if city:
#             conditions.append(SalesAllInOneLive.city == city)
#         if demo_flag:
#             conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

#         if price_breakup2:
#             sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
#             if price_breakup2 == '0-1000':
#                 conditions.append(sales_per_unit <= 1000)
#             elif price_breakup2 == '1001-2000':
#                 conditions.append(sales_per_unit.between(1000, 2000))
#             elif price_breakup2 == '2001-3000':
#                 conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
#             elif price_breakup2 == '3001-4000':
#                 conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
#             elif price_breakup2 == '4001-5000':
#                 conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
#             elif price_breakup2 == '5001-6000':
#                 conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
#             elif price_breakup2 == '6001-7000':
#                 conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
#             elif price_breakup2 == '7001-8000':
#                 conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
#             elif price_breakup2 == '8001-9000':
#                 conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
#             elif price_breakup2 == '9001-10000':
#                 conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
#             elif price_breakup2 == '10001-20000':
#                 conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
#             elif price_breakup2 == '20001-30000':
#                 conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
#             elif price_breakup2 == '30001-40000':
#                 conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
#             elif price_breakup2 == '40001-50000':
#                 conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
#             elif price_breakup2 == '>50000':
#                 conditions.append(sales_per_unit > 50000)

#         # Retrieve and validate the factor
#         factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

#         if factor == 'cr':
#             value = 10000000
#         elif factor == 'cr_without_gst':
#             value = 10000000
#         elif factor == 'lk':
#             value = 100000
#         elif factor == 'lk_without_gst':
#             value = 100000
#         elif factor == 'sales_qty':
#             value = None  # Handle separately, as sales_qty doesn't need a factor
#         elif factor == 'total_sales':
#             value = None  # Handle separately
#         elif factor == 'gp':
#             value = 100000
#         else:
#             return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

#         sales_data = db.session.query(
#             extract("year", SalesAllInOneLive.invoice_date).label("year"),
#             extract("month", SalesAllInOneLive.invoice_date).label("month"),
#             func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
#             func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
#             func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
#             func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
#         ).filter(*conditions)

#         sales_data = sales_data.group_by(
#             extract("year", SalesAllInOneLive.invoice_date),
#             extract("month", SalesAllInOneLive.invoice_date)
#         ).all()

#         result_dict = {}

#         month_names = {
#             4: "Apr",
#             5: "May",
#             6: "Jun",
#             7: "Jul",
#             8: "Aug",
#             9: "Sep",
#             10: "Oct",
#             11: "Nov",
#             12: "Dec",
#             1: "Jan",
#             2: "Feb",
#             3: "Mar",
#         }

#         price_ranges = {
#                 "Null": {},
#                 "0 - 5000": {},
#                 "5001 - 10000": {},
#                 "10001 - 15000": {},
#                 "15001 - 20000": {},
#                 "20001 - 25000": {},
#                 "25001 - 50000": {},
#                 "50001 - 70000": {},
#                 "70001 - 100000": {},
#                 ">100000": {}
#         }

#         total_sales_by_year = defaultdict(float)
#         years_set = set()

#         for year, month, total_sales, total_qty, tax_amt, gros_profit in sales_data:
#             if month in [1, 2, 3]:
#                 fiscal_year = year
#             else:
#                 fiscal_year = year + 1

#             financial_month = month_names[month]
#             years_set.add(fiscal_year)

#             # Default values in case of None
#             total_sales = total_sales or 0
#             total_qty = total_qty or 0
#             tax_amt = tax_amt or 0
#             gros_profit = gros_profit or 0

#             # Calculate sales details based on the selected factor
#             if factor == 'sales_qty':
#                 sales_details = total_qty
#             elif factor == 'total_sales':
#                 sales_details = total_sales
#             elif factor == 'gp':
#                 sales_details = round(gros_profit / value, 2) if value else gros_profit
#             else:
#                 if factor in ['cr', 'cr_without_gst']:
#                     if factor == 'cr_without_gst':
#                         sales_details = round((total_sales - tax_amt) / value, 2)
#                     else:
#                         sales_details = round(total_sales / value, 2)
#                 elif factor in ['lk', 'lk_without_gst']:
#                     if factor == 'lk_without_gst':
#                         sales_details = round((total_sales - tax_amt) / value, 2)
#                     else:
#                         sales_details = round(total_sales / value, 2)

#             sales_with_gst = sales_details

#             piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
#             total_sales_by_year[fiscal_year] += sales_with_gst

#             if piecewise_sales <= 0:
#                 price_breakup = "Null"
#             elif piecewise_sales > 0 and piecewise_sales <= 5000:
#                 price_breakup = "0 - 5000"
#             elif piecewise_sales > 5000 and piecewise_sales <= 10000:
#                 price_breakup = "5001 - 10000"
#             elif piecewise_sales > 10000 and piecewise_sales <= 15000:
#                 price_breakup = "10001 - 15000"
#             elif piecewise_sales > 15000 and piecewise_sales <= 20000:
#                 price_breakup = "15001 - 20000"
#             elif piecewise_sales > 20000 and piecewise_sales <= 25000:
#                 price_breakup = "20001 - 25000"
#             elif piecewise_sales > 25000 and piecewise_sales <= 50000:
#                 price_breakup = "25001 - 50000"
#             elif piecewise_sales > 50000 and piecewise_sales <= 70000:
#                 price_breakup = "50001 - 70000"
#             elif piecewise_sales > 70000 and piecewise_sales <= 100000:
#                 price_breakup = "70001 - 100000"
#             elif piecewise_sales > 100000:
#                 price_breakup = ">100000"

#             if price_breakup != "Null":
#                 if fiscal_year not in price_ranges[price_breakup]:
#                     price_ranges[price_breakup][fiscal_year] = 0
#                 price_ranges[price_breakup][fiscal_year] += sales_with_gst

#         years_list = sorted(years_set, reverse=True)

#         for price_range, sales_data in price_ranges.items():
#             for year in years_list:
#                 sales_value = sales_data.get(year, 0)
#                 total_sales = total_sales_by_year[year]
#                 percentage = (
#                     round((sales_value / total_sales) * 100, 2)
#                     if total_sales > 0
#                     else 0
#                 )
#                 sv = round(sales_value,2)

#                 sales_data[year] = f"{sv} ({percentage}%)"

#         return jsonify({"years": years_list, "values": price_ranges}), 200

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_sales_all_in_one_live_price_breakup_one_cr_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})
def get_sales_all_in_one_live_price_breakup_one_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        gstfilter = request.args.get('gstfilter','cr')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges
    
            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Retrieve and validate the factor
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately, as sales_qty doesn't need a factor
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        price_breakup = case(
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 5000, '0-5000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 10000, '5001-10000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 15000, '10001-15000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 20000, '15001-20000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 25000, '20001-25000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 50000, '25001-50000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 70000, '50001-70000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 100000, '70001-100000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 100000, '>100000')
        )

        # Determine the fiscal year
        fiscal_year = case(
            (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
            (func.month(SalesAllInOneLive.invoice_date) < 4, func.year(SalesAllInOneLive.invoice_date))
        )

        # Query the database
        result = db.session.query(
            fiscal_year.label('fiscal_year'),
            price_breakup.label('price_breakup'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('total_qty'),
            func.sum(SalesAllInOneLive.tax_amt).label('tax_amt'),
            func.sum(SalesAllInOneLive.gros_profit).label('gros_profit')
        ).filter(*conditions).group_by(
            fiscal_year, price_breakup
        ).order_by(
            fiscal_year.desc()
        ).all()

        # Initialize output dictionary
        output = {"values": {}, "years": sorted({row.fiscal_year for row in result}, reverse=True)}
        outputs = {"values": {}, "years": sorted({row.fiscal_year for row in result}, reverse=True)}

        # Define price breakup categories
        price_breakups = [
            '0-5000', '5001-10000', '10001-15000', '15001-20000', '20001-25000',
            '25001-50000', '50001-70000', '70001-100000', '>100000'
        ]

        # Initialize values dictionary
        for price in price_breakups:
            output["values"][price] = {year: "0 (0.0%)" for year in output["years"]}
            outputs["values"][price] = {year: "0 (0.0%)" for year in output["years"]}

        # Process results
        for row in result:
            fiscal_year = row.fiscal_year
            price_breakup = row.price_breakup
            
            # Default values in case of None
            total_sales = row.total_sales or 0
            total_qty = row.total_qty or 0
            tax_amt = row.tax_amt or 0
            gros_profit = row.gros_profit or 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = total_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 3) if value else gros_profit
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 3)
                    else:
                        sales_details = round(total_sales / value, 3)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 3)
                    else:
                        sales_details = round(total_sales / value, 3)

            sales_with_gst = sales_details    
            total_sales_for_year = sum([r.total_sales for r in result if r.fiscal_year == fiscal_year])
            percentage = (Decimal(sales_with_gst) / Decimal(total_sales_for_year) * Decimal(100)) if Decimal(total_sales_for_year) > 0 else Decimal(0)
            
            # Format the value and percentage
            formatted_value = f"{sales_with_gst:.2f} ({percentage:.2f}%)"
            output["values"][price_breakup][fiscal_year] = formatted_value

        yearly_totals = {year: Decimal(0) for year in output["years"]}
        numeric_values = {year: [] for year in output["years"]}

        # Iterate over each range and each year to sum up the numeric values
        for key, value_dict in output["values"].items():
            for year, value in value_dict.items():
                # Extract the numeric value before the percentage
                numeric_value = Decimal(value.split(' ')[0])
                yearly_totals[int(year)] += numeric_value
                numeric_values[int(year)].append(numeric_value)

        # Calculate percentage for each numeric_value
        percentage_results = {}
        for year, values in numeric_values.items():
            total_sales_for_year = yearly_totals[year]
            percentages = [
                (numeric_value / total_sales_for_year * Decimal(100)) if total_sales_for_year > 0 else Decimal(0)
                for numeric_value in values
            ]
            percentage_results[year] = percentages

        # Prepare the output structure
        outputs = {"values": {}}

        # Format the numeric values and percentages and store them in the output
        for year in output["years"]:
            for i, numeric_value in enumerate(numeric_values[year]):
                percentage = percentage_results[year][i]
                formatted_values = f"{numeric_value:.2f} ({percentage:.2f}%)"
                
                # Assuming price_breakup and fiscal_year are the keys for your output
                price_breakup = list(output["values"].keys())[i]  # Example: '0-5000'
                fiscal_year = year  # Use the actual year
                
                # Store formatted values in the output dictionary
                if price_breakup not in outputs["values"]:
                    outputs["values"][price_breakup] = {}
                outputs["values"][price_breakup][fiscal_year] = formatted_values

        # Return JSON response
        years_list = sorted(output["years"], reverse=True)
        return jsonify({"years": years_list, "values": outputs["values"]}), 200

    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# --------------------------------------- Price Breakup 2 ------------------------------------

def get_sales_all_in_one_live_price_breakup_two_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        gstfilter = request.args.get('gstfilter','cr')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges
    
            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Retrieve and validate the factor
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately, as sales_qty doesn't need a factor
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        price_breakup = case(
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 1000, '0-1000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 2000, '1001-2000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 3000, '2001-3000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 4000, '3001-4000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 5000, '4001-5000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 6000, '5001-6000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 7000, '6001-7000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 8000, '7001-8000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 9000, '8001-9000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 10000, '9001-10000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 20000, '10001-20000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 30000, '20001-30000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 40000, '30001-40000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty <= 50000, '40001-50000'),
            (SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty > 50000, '>50000')
        )
        
        # Perform the query with aggregation
        # Determine the fiscal year
        fiscal_year = case(
            (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
            (func.month(SalesAllInOneLive.invoice_date) < 4, func.year(SalesAllInOneLive.invoice_date))
        )

        # Query the database
        result = db.session.query(
            fiscal_year.label('fiscal_year'),
            price_breakup.label('price_breakup'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('total_qty'),
            func.sum(SalesAllInOneLive.tax_amt).label('tax_amt'),
            func.sum(SalesAllInOneLive.gros_profit).label('gros_profit')
        ).filter(*conditions).group_by(
            fiscal_year, price_breakup
        ).order_by(
            fiscal_year.desc()
        ).all()

        # Prepare the years and months
        years_set = set()
        month_names = [
            'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 
            'September', 'October', 'November', 'December'
        ]
        
        # Initialize output dictionary
        output = {"values": {}, "years": sorted({row.fiscal_year for row in result}, reverse=True)}
        outputs = {"values": {}, "years": sorted({row.fiscal_year for row in result}, reverse=True)}

        price_breakups = [
            '0-1000', '1001-2000', '2001-3000', '3001-4000', '4001-5000',
            '5001-6000', '6001-7000', '7001-8000','8001-9000','9001-10000','10001-20000','20001-30000','30001-40000','40001-50000','>50000'
        ]
        
        # Initialize values dictionary for price_breakups and years
        # Initialize values dictionary
        for price in price_breakups:
            output["values"][price] = {year: "0 (0.0%)" for year in output["years"]}
            outputs["values"][price] = {year: "0 (0.0%)" for year in output["years"]}

        # Process results
        for row in result:
            fiscal_year = row.fiscal_year
            price_breakup = row.price_breakup
            
            # Default values in case of None
            total_sales = row.total_sales or 0
            total_qty = row.total_qty or 0
            tax_amt = row.tax_amt or 0
            gros_profit = row.gros_profit or 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = total_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 3) if value else gros_profit
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 3)
                    else:
                        sales_details = round(total_sales / value, 3)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 3)
                    else:
                        sales_details = round(total_sales / value, 3)

            sales_with_gst = sales_details    
            total_sales_for_year = sum([r.total_sales for r in result if r.fiscal_year == fiscal_year])
            percentage = (Decimal(sales_with_gst) / Decimal(total_sales_for_year) * Decimal(100)) if Decimal(total_sales_for_year) > 0 else Decimal(0)
            
            # Format the value and percentage
            formatted_value = f"{sales_with_gst:.2f} ({percentage:.2f}%)"
            output["values"][price_breakup][fiscal_year] = formatted_value

        yearly_totals = {year: Decimal(0) for year in output["years"]}
        numeric_values = {year: [] for year in output["years"]}

        # Iterate over each range and each year to sum up the numeric values
        for key, value_dict in output["values"].items():
            for year, value in value_dict.items():
                # Extract the numeric value before the percentage
                numeric_value = Decimal(value.split(' ')[0])
                yearly_totals[int(year)] += numeric_value
                numeric_values[int(year)].append(numeric_value)

        # Calculate percentage for each numeric_value
        percentage_results = {}
        for year, values in numeric_values.items():
            total_sales_for_year = yearly_totals[year]
            percentages = [
                (numeric_value / total_sales_for_year * Decimal(100)) if total_sales_for_year > 0 else Decimal(0)
                for numeric_value in values
            ]
            percentage_results[year] = percentages

        # Prepare the output structure
        outputs = {"values": {}}

        # Format the numeric values and percentages and store them in the output
        for year in output["years"]:
            for i, numeric_value in enumerate(numeric_values[year]):
                percentage = percentage_results[year][i]
                formatted_values = f"{numeric_value:.2f} ({percentage:.2f}%)"
                
                # Assuming price_breakup and fiscal_year are the keys for your output
                price_breakup = list(output["values"].keys())[i]  # Example: '0-5000'
                fiscal_year = year  # Use the actual year
                
                # Store formatted values in the output dictionary
                if price_breakup not in outputs["values"]:
                    outputs["values"][price_breakup] = {}
                outputs["values"][price_breakup][fiscal_year] = formatted_values

        # Return JSON response
        years_list = sorted(output["years"], reverse=True)
        return jsonify({"years": years_list, "values": outputs["values"]}), 200
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

    # try:
          
    #     period_from = request.args.get("period_from")
    #     period_to = request.args.get("period_to")
    #     invoice_date = request.args.get('invoice_date')
    #     srn_flag = request.args.get('srn_flag')
    #     sales_type = request.args.get('sales_type')
    #     section = request.args.get('section')
    #     product_group = request.args.get('product_group')
    #     item_category = request.args.get('item_category')
    #     brand_name = request.args.get('brand_name')
    #     model_no = request.args.get('model_no')
    #     item_description = request.args.get('item_description')
    #     store_name = request.args.get('store_name')
    #     city = request.args.get('city')
    #     demo_flag = request.args.get('demo_flag')
    #     price_breakup2 = request.args.get('PriceBreakup2')
    #     asm = request.args.get('asm')
    #     store_code = request.args.get('storecode')

    #     # Initialize conditions
    #     conditions = []

    #     # Apply dynamic conditions
    #     if asm:
    #         conditions.append(SalesAllInOneLive.asm == asm)
    #     if store_code:
    #         conditions.append(SalesAllInOneLive.store_code == store_code)
    #     if period_from:
    #         conditions.append(SalesAllInOneLive.invoice_date >= period_from)
    #     if period_to:
    #         conditions.append(SalesAllInOneLive.invoice_date <= period_to)
    #     if invoice_date:
    #         conditions.append(SalesAllInOneLive.invoice_date == invoice_date)
    #     if srn_flag:
    #         conditions.append(SalesAllInOneLive.srn_flag == srn_flag)
    #     if sales_type:
    #         conditions.append(SalesAllInOneLive.sale_type == sales_type)
    #     if section:
    #         conditions.append(SalesAllInOneLive.section == section)
    #     if product_group:
    #         conditions.append(SalesAllInOneLive.product_group == product_group)
    #     if item_category:
    #         conditions.append(SalesAllInOneLive.item_category == item_category)
    #     if brand_name:
    #         conditions.append(SalesAllInOneLive.brand_name == brand_name)
    #     if model_no:
    #         conditions.append(SalesAllInOneLive.model_no == model_no)
    #     if item_description:
    #         conditions.append(SalesAllInOneLive.item_description == item_description)
    #     if store_name:
    #         conditions.append(SalesAllInOneLive.store_name == store_name)
    #     if city:
    #         conditions.append(SalesAllInOneLive.city == city)
    #     if demo_flag:
    #         conditions.append(SalesAllInOneLive.demo_flag == demo_flag)

    #     if price_breakup2:
    #         sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
    #         if price_breakup2 == '0-1000':
    #             conditions.append(sales_per_unit <= 1000)
    #         elif price_breakup2 == '1001-2000':
    #             conditions.append(sales_per_unit.between(1000, 2000))
    #         elif price_breakup2 == '2001-3000':
    #             conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
    #         elif price_breakup2 == '3001-4000':
    #             conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
    #         elif price_breakup2 == '4001-5000':
    #             conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
    #         elif price_breakup2 == '5001-6000':
    #             conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
    #         elif price_breakup2 == '6001-7000':
    #             conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
    #         elif price_breakup2 == '7001-8000':
    #             conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
    #         elif price_breakup2 == '8001-9000':
    #             conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
    #         elif price_breakup2 == '9001-10000':
    #             conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
    #         elif price_breakup2 == '10001-20000':
    #             conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
    #         elif price_breakup2 == '20001-30000':
    #             conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
    #         elif price_breakup2 == '30001-40000':
    #             conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
    #         elif price_breakup2 == '40001-50000':
    #             conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
    #         elif price_breakup2 == '>50000':
    #             conditions.append(sales_per_unit > 50000)

    #     gstfilter = request.args.get('gstfilter', 'cr')  # Default to 'cr'

    #     # Determine value based on gstfilter
    #     if gstfilter == 'cr':
    #         value = 10000000
    #     elif gstfilter == 'cr_without_gst':
    #         value = 10000000
    #     elif gstfilter == 'lk':
    #         value = 100000
    #     elif gstfilter == 'lk_without_gst':
    #         value = 100000
    #     elif gstfilter == 'sales_qty':
    #         value = None  # Handle separately
    #     elif gstfilter == 'total_sales':
    #         value = None  # Handle separately
    #     elif gstfilter == 'gp':
    #         value = 100000
    #     else:
    #         return jsonify({"success": 0, "error": f"Invalid gstfilter: {gstfilter}"}), 400

    #     sales_data = db.session.query(
    #         extract("year", SalesAllInOneLive.invoice_date).label("year"),
    #         extract("month", SalesAllInOneLive.invoice_date).label("month"),
    #         func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
    #         func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
    #         func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
    #         func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
    #     ).filter(*conditions)

    #     sales_data = sales_data.group_by(
    #         extract("year", SalesAllInOneLive.invoice_date),
    #         extract("month", SalesAllInOneLive.invoice_date)
    #     ).all()

    #     result_dict = {}

    #     price_ranges = {
    #         "0 - 1000": {},
    #         "1001 - 2000": {},
    #         "2001 - 3000": {},
    #         "3001 - 4000": {},
    #         "4001 - 5000": {},
    #         "5001 - 6000": {},
    #         "6001 - 7000": {},
    #         "7001 - 8000": {},
    #         "8001 - 9000": {},
    #         "9001 - 10000": {},
    #         "10001 - 20000": {},
    #         "20001 - 30000": {},
    #         "30001 - 40000": {},
    #         "40001 - 50000": {},
    #         ">50000": {},
    #     }

    #     years_set = set()

    #     total_sales_by_year = defaultdict(float)

    #     for year, month, total_sales, total_qty, tax_amt, gros_profit in sales_data:
    #         if total_sales is None:
    #             total_sales = 0
    #         if total_qty is None:
    #             total_qty = 0
    #         if tax_amt is None:
    #             tax_amt = 0
    #         if gros_profit is None:
    #             gros_profit = 0

    #         # Calculate sales details based on the selected gstfilter
    #         if gstfilter == 'sales_qty':
    #             sales_details = total_qty
    #         elif gstfilter == 'total_sales':
    #             sales_details = total_sales
    #         elif gstfilter == 'gp':
    #             sales_details = round(gros_profit / value, 2)
    #         else:
    #             if gstfilter in ['cr', 'cr_without_gst']:
    #                 if gstfilter == 'cr_without_gst':
    #                     sales_details = round((total_sales - tax_amt) / value, 2)
    #                 else:
    #                     sales_details = round(total_sales / value, 2)
    #             elif gstfilter in ['lk', 'lk_without_gst']:
    #                 if gstfilter == 'lk_without_gst':
    #                     sales_details = round((total_sales - tax_amt) / value, 2)
    #                 else:
    #                     sales_details = round(total_sales / value, 2)

    #         sales_with_gst = sales_details

    #         piecewise_sales = total_sales / total_qty if total_qty > 0 else 0

    #         if month in [1, 2, 3]:
    #             fiscal_year = year
    #         else:
    #             fiscal_year = year + 1

    #         years_set.add(fiscal_year)

    #         total_sales_by_year[fiscal_year] += sales_with_gst

    #         price_breakup = "Null"
    #         if piecewise_sales > 0 and piecewise_sales <= 1000:
    #             price_breakup = "0 - 1000"
    #         elif piecewise_sales > 1000 and piecewise_sales <= 2000:
    #             price_breakup = "1001 - 2000"
    #         elif piecewise_sales > 2000 and piecewise_sales <= 3000:
    #             price_breakup = "2001 - 3000"
    #         elif piecewise_sales > 3000 and piecewise_sales <= 4000:
    #             price_breakup = "3001 - 4000"
    #         elif piecewise_sales > 4000 and piecewise_sales <= 5000:
    #             price_breakup = "4001 - 5000"
    #         elif piecewise_sales > 5000 and piecewise_sales <= 6000:
    #             price_breakup = "5001 - 6000"
    #         elif piecewise_sales > 6000 and piecewise_sales <= 7000:
    #             price_breakup = "6001 - 7000"
    #         elif piecewise_sales > 7000 and piecewise_sales <= 8000:
    #             price_breakup = "7001 - 8000"
    #         elif piecewise_sales > 8000 and piecewise_sales <= 9000:
    #             price_breakup = "8001 - 9000"
    #         elif piecewise_sales > 9000 and piecewise_sales <= 10000:
    #             price_breakup = "9001 - 10000"
    #         elif piecewise_sales > 10000 and piecewise_sales <= 20000:
    #             price_breakup = "10001 - 20000"
    #         elif piecewise_sales > 20000 and piecewise_sales <= 30000:
    #             price_breakup = "20001 - 30000"
    #         elif piecewise_sales > 30000 and piecewise_sales <= 40000:
    #             price_breakup = "30001 - 40000"
    #         elif piecewise_sales > 40000 and piecewise_sales <= 50000:
    #             price_breakup = "40001 - 50000"
    #         elif piecewise_sales > 50000:
    #             price_breakup = ">50000"

    #         if price_breakup != "Null":
    #             if fiscal_year not in price_ranges[price_breakup]:
    #                 price_ranges[price_breakup][fiscal_year] = 0
    #             price_ranges[price_breakup][fiscal_year] += sales_with_gst

    #     years_list = sorted(years_set, reverse=True)

    #     for price_range, sales_data in price_ranges.items():
    #         for year in years_list:
    #             sales_value = sales_data.get(year, 0)
    #             total_sales = total_sales_by_year[year]
    #             percentage = (
    #                 round((sales_value / total_sales) * 100, 2)
    #                 if total_sales > 0
    #                 else 0
    #             )
    #             sv= round(sales_value,2)
    #             sales_data[year] = f"{sv} ({percentage}%)"

    #     return jsonify({"years": years_list, "values": price_ranges}), 200

    # except Exception as e:
    #     db.session.rollback()
    #     if "MySQL server has gone away" in str(e):
    #         return get_sales_all_in_one_live_price_breakup_two_cr_controller()
    #     else:
    #         return jsonify({"success": 0, "error": str(e)})

def get_sales_all_in_column_live_controller():
    try:
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Define the columns to fetch with aggregation
        columns = [
            SalesAllInOneLive.store_name,
            SalesAllInOneLive.city,
            SalesAllInOneLive.section,
            SalesAllInOneLive.item_description,
            SalesAllInOneLive.product_group,
            SalesAllInOneLive.brand_name,
            SalesAllInOneLive.model_no,
            SalesAllInOneLive.demo_flag,
            SalesAllInOneLive.srn_flag,
            SalesAllInOneLive.sale_type,
            SalesAllInOneLive.item_category,
        ]

        # Perform optimized query with distinct aggregation
        query = db.session.query(*columns).filter(*conditions).distinct()

        # Utilize server-side pagination for massive data (optional)
        results = query.limit(10000).all()  # Adjust the limit as needed

        # Process the results dynamically
        sales_data = {}
        for column in columns:
            sales_data[column.key] = set()

        for record in results:
            for column, value in zip(columns, record):
                sales_data[column.key].add(value)

        # Convert sets to lists for JSON serialization
        sales_data = {key: sorted(value) for key, value in sales_data.items()}

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry logic for database connection error
            return get_sales_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def get_sales_all_in_date_controller():
    try:
        # Query to get the min and max invoice_date from the sales_all_in_one_live table (no additional conditions)
        start_end_dates = db.session.query(
            func.min(SalesAllInOneLive.invoice_date).label('start_date'),
            func.max(SalesAllInOneLive.invoice_date).label('end_date')
        ).one()

        start_date = start_end_dates.start_date
        end_date = start_end_dates.end_date

        # Format the start_date and end_date to "25 -11 -2024" format
        start_date_str = start_date.strftime("%d -%m -%Y") if start_date else None
        end_date_str = end_date.strftime("%d -%m -%Y") if end_date else None

        # Return the formatted start_date and end_date
        result_data = [{
            "start_date": start_date_str,
            "end_date": end_date_str
        }]

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_date_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def get_sales_all_in_one_live_section_dimension_cr_controller():
    try:
        # Get request parameters
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        # Map factor to corresponding value
        factor_values = {
            'cr': 10000000,
            'cr_without_gst': 10000000,
            'lk': 100000,
            'lk_without_gst': 100000,
            'sales_qty': None,
            'total_sales': None,
            'gp': 100000
        }

        value = factor_values.get(factor)
        if value is None and factor not in ['sales_qty', 'total_sales']:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        # Apply filtering conditions

        # Query sales data with pagination
        sales_data = db.session.query(
            SalesAllInOneLive.section,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            SalesAllInOneLive.section,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date)
        ).order_by(SalesAllInOneLive.section).limit(limit).offset(offset).all()

        sales_datas1 = db.session.query(
            SalesAllInOneLive.section,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit")
        ).filter(*conditions)

        sales_datas1 = sales_datas1.group_by(
            SalesAllInOneLive.section,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date)
        ).order_by(SalesAllInOneLive.section)

        sales_data1 = sales_datas1.all()

        # Initialize result structures
        result_dict = {}
        month_names = {
            4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep",
            10: "Oct", 11: "Nov", 12: "Dec", 1: "Jan", 2: "Feb", 3: "Mar"
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }

        years_list = []
        yearly_totals = {}

        # Process sales data
        for section, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = total_sales or 0
            sales_qty = sales_qty or 0
            tax_amt = tax_amt or 0
            gros_profit = gros_profit or 0

            # Calculate sales details based on factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor.endswith('_without_gst'):
                    sales_details = round((total_sales - tax_amt) / value, 2)
                else:
                    sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            # Update result_dict and yearly_totals
            result_dict.setdefault(section, {}).setdefault(fiscal_year, {})[financial_month] = {
                "sales_with_gst": sales_with_gst
            }
            yearly_totals.setdefault(section, {}).setdefault(fiscal_year, 0)
            yearly_totals[section][fiscal_year] = Decimal(yearly_totals[section][fiscal_year])
            yearly_totals[section][fiscal_year] += Decimal(sales_with_gst)

        monthly_totals = {}
        for section, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data1:

            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if factor == 'cr':
                value = 10000000
            elif factor == 'cr_without_gst':
                value = 10000000
            elif factor == 'lk':
                value = 100000
            elif factor == 'lk_without_gst':
                value = 100000
            elif factor == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif factor == 'total_sales':
                value = None  # Handle separately
            elif factor == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                    sales_details = sales_qty
            elif factor == 'total_sales':
                    sales_details = total_sales
            elif factor == 'gp':
                    sales_details = gros_profit / value
            else:
                    if factor in ['cr', 'cr_without_gst']:
                        if factor == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif factor in ['lk', 'lk_without_gst']:
                        if factor == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)
        # monthly_totals = {}
        # for section, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         for month, data in months_data.items():
        #             if fiscal_year not in monthly_totals:
        #                 monthly_totals[fiscal_year] = {}
                    
        #             if month not in monthly_totals[fiscal_year]:
        #                 monthly_totals[fiscal_year][month] = 0

        #             # Add up sales for each month
        #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
        #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
        for section, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[section][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[section][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"
        # Calculate percentages and format results
        # for section, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = Decimal(yearly_totals[section][fiscal_year])
        #         for month, data in months_data.items():
        #             sales_with_gst = Decimal(data["sales_with_gst"])
        #             percentage = round((sales_with_gst / yearly_total) * 100, 2) if yearly_total > 0 else 0.0
        #             result_dict[section][fiscal_year][month] = f"{round(sales_with_gst,2)} ({percentage}%)"

        years_list.sort(reverse=True)
        return jsonify({"years": years_list, "values": result_dict,
            "yearly_totals": yearly_totals}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_section_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)}), 500

def get_sales_all_in_one_live_itemcategory_dimension_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')
        factor = request.args.get('gstfilter')  # Default to 'cr'
        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        # Determine the scaling factor
        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately
        elif factor == 'total_sales':
            # print("exclude_modal_number2")
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        # Query to get the sales data with pagination and sorting by brand_name
        sales_data = db.session.query(
            SalesAllInOneLive.item_category,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.item_category,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.item_category).offset(offset).limit(limit).all()

        sales_datas1 = db.session.query(
            SalesAllInOneLive.item_category,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.item_category,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.item_category)

        sales_data1 = sales_datas1.all()

        # Initialize result dict and year mapping
        result_dict = {}
        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }
        years_list = []
        yearly_totals = {}

        # Process sales data and organize into result_dict
        for item_category, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            if total_sales is None:
                total_sales = 0
                sales_qty = 0
                tax_amt = 0
                gros_profit = 0

            if gros_profit is None:
                gros_profit = 0
            if total_sales is None:
                total_sales = 0
            if tax_amt is None:
                tax_amt = 0

            # Calculate sales details based on the selected factor
            if value is None or value == 0:
                  if factor == 'sales_qty':
                   sales_details = sales_qty
                  elif factor == 'total_sales':
                   sales_details = total_sales
            elif factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor in ['cr', 'cr_without_gst']:                    
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            if item_category not in result_dict:
                result_dict[item_category] = {}

            if fiscal_year not in result_dict[item_category]:
                result_dict[item_category][fiscal_year] = {}

            if item_category not in yearly_totals:
                yearly_totals[item_category] = {}

            if fiscal_year not in yearly_totals[item_category]:
                yearly_totals[item_category][fiscal_year] = 0
            
            yearly_totals[item_category][fiscal_year] = Decimal(yearly_totals[item_category][fiscal_year])
            yearly_totals[item_category][fiscal_year] += Decimal(sales_with_gst)

            result_dict[item_category][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        monthly_totals = {}
        for item_category, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data1:

            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if factor == 'cr':
                value = 10000000
            elif factor == 'cr_without_gst':
                value = 10000000
            elif factor == 'lk':
                value = 100000
            elif factor == 'lk_without_gst':
                value = 100000
            elif factor == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif factor == 'total_sales':
                value = None  # Handle separately
            elif factor == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                    sales_details = sales_qty
            elif factor == 'total_sales':
                    sales_details = total_sales
            elif factor == 'gp':
                    sales_details = gros_profit / value
            else:
                    if factor in ['cr', 'cr_without_gst']:
                        if factor == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif factor in ['lk', 'lk_without_gst']:
                        if factor == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)

        # monthly_totals = {}
        # for item_category, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         for month, data in months_data.items():
        #             if fiscal_year not in monthly_totals:
        #                 monthly_totals[fiscal_year] = {}
                    
        #             if month not in monthly_totals[fiscal_year]:
        #                 monthly_totals[fiscal_year][month] = 0

        #             # Add up sales for each month
        #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
        #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
        for item_category, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[item_category][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[item_category][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"
        # Calculate percentage of sales for each month and add to result_dict
        # for item_category, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = Decimal(yearly_totals[item_category][fiscal_year])
        #         for month, data in months_data.items():
        #             sales_with_gst = Decimal(data['sales_with_gst'])
        #             if sales_with_gst >= 0:
        #                 result_dict[item_category][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
        #             else:
        #                 percentage = round((sales_with_gst / yearly_total) * 100, 2)
        #                 result_dict[item_category][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"

        years_list.reverse()

        # Return the result as JSON with pagination
        return jsonify({
            "years": years_list,
            "values": result_dict,
            "yearly_totals": yearly_totals,
            "page": page,
            "limit": limit,
        }), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_itemcategory_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# --------------------------Branch Dimension -----------------

def get_sales_all_in_one_live_branch_dimension_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Get filters from request args
        gstfilter = request.args.get('gstfilter', 'cr')  # Default to 'cr'
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

        # Determine the scaling factor
        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        # Query to get the sales data with pagination and sorting by brand_name
        sales_data = db.session.query(
            SalesAllInOneLive.store_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.store_name,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.store_name).offset(offset).limit(limit).all()

        sales_datas1 = db.session.query(
            SalesAllInOneLive.store_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.store_name,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.store_name)

        sales_data1 =sales_datas1.all()

        # Initialize result dict and year mapping
        result_dict = {}
        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }
        years_list = []
        yearly_totals = {}

        # Process sales data and organize into result_dict
        for store_name, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            if total_sales is None:
                total_sales = 0
                sales_qty = 0
                tax_amt = 0
                gros_profit = 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            if store_name not in result_dict:
                result_dict[store_name] = {}

            if fiscal_year not in result_dict[store_name]:
                result_dict[store_name][fiscal_year] = {}

            if store_name not in yearly_totals:
                yearly_totals[store_name] = {}

            if fiscal_year not in yearly_totals[store_name]:
                yearly_totals[store_name][fiscal_year] = 0

            yearly_totals[store_name][fiscal_year] += sales_with_gst

            result_dict[store_name][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        monthly_totals = {}
        for store_name, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data1:
            

            
            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if gstfilter == 'cr':
                value = 10000000
            elif gstfilter == 'cr_without_gst':
                value = 10000000
            elif gstfilter == 'lk':
                value = 100000
            elif gstfilter == 'lk_without_gst':
                value = 100000
            elif gstfilter == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif gstfilter == 'total_sales':
                value = None  # Handle separately
            elif gstfilter == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {gstfilter}"}), 400

            # Calculate sales details based on the selected factor
            if gstfilter == 'sales_qty':
                    sales_details = sales_qty
            elif gstfilter == 'total_sales':
                    sales_details = total_sales
            elif gstfilter == 'gp':
                    sales_details = gros_profit / value
            else:
                    if gstfilter in ['cr', 'cr_without_gst']:
                        if gstfilter == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif gstfilter in ['lk', 'lk_without_gst']:
                        if gstfilter == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)

        # monthly_totals = {}
        # for store_name, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         for month, data in months_data.items():
        #             if fiscal_year not in monthly_totals:
        #                 monthly_totals[fiscal_year] = {}
                    
        #             if month not in monthly_totals[fiscal_year]:
        #                 monthly_totals[fiscal_year][month] = 0

        #             # Add up sales for each month
        #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
        #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
        for store_name, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[store_name][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[store_name][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"
        # Calculate percentage of sales for each month and add to result_dict
        # for store_name, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = yearly_totals[store_name][fiscal_year]
        #         for month, data in months_data.items():
        #             if data["sales_with_gst"] == 0:
        #                 result_dict[store_name][fiscal_year][month] = f"{data['sales_with_gst']} ({0.00}%)"
        #             else:
        #                 percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
        #                 result_dict[store_name][fiscal_year][month] = f"{data['sales_with_gst']} ({percentage}%)"

        years_list.reverse()

        # Return the result as JSON with pagination
        return jsonify({
            "years": years_list,
            "values": result_dict,
            "yearly_totals": yearly_totals,
            "page": page,
            "limit": limit,
        }), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_branch_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -----------------------------City Dimension-------------------------

def get_sales_all_in_one_live_city_dimension_cr_controller():
    try:
          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        product_group = request.args.get('product_group')
        item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        if asm:
            conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        if product_group:
            product_group_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        if item_category:
            item_category_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        if store_name:
            store_name_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        if city:
            city_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list =  re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))


        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-1000':
                    price_conditions.append(sales_per_unit <= 1000)
                elif price_range == '1001-2000':
                    price_conditions.append(sales_per_unit.between(1000, 2000))
                elif price_range == '2001-3000':
                    price_conditions.append((sales_per_unit > 2000) & (sales_per_unit <= 3000))
                elif price_range == '3001-4000':
                    price_conditions.append((sales_per_unit > 3000) & (sales_per_unit <= 4000))
                elif price_range == '4001-5000':
                    price_conditions.append((sales_per_unit > 4000) & (sales_per_unit <= 5000))
                elif price_range == '5001-6000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 6000))
                elif price_range == '6001-7000':
                    price_conditions.append((sales_per_unit > 6000) & (sales_per_unit <= 7000))
                elif price_range == '7001-8000':
                    price_conditions.append((sales_per_unit > 7000) & (sales_per_unit <= 8000))
                elif price_range == '8001-9000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 9000))
                elif price_range == '9001-10000':
                    price_conditions.append((sales_per_unit > 9000) & (sales_per_unit <= 10000))
                elif price_range == '10001-20000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-50000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 50000))
                elif price_range == '>50000':
                    price_conditions.append(sales_per_unit > 50000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        # Get filters from request args
        factor = request.args.get('gstfilter', 'cr')  # Default to 'cr'

        # Determine the scaling factor
        if factor == 'cr':
            value = 10000000
        elif factor == 'cr_without_gst':
            value = 10000000
        elif factor == 'lk':
            value = 100000
        elif factor == 'lk_without_gst':
            value = 100000
        elif factor == 'sales_qty':
            value = None  # Handle separately
        elif factor == 'total_sales':
            value = None  # Handle separately
        elif factor == 'gp':
            value = 100000
        else:
            return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

        # Pagination
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit

        # Query to get the sales data with pagination and sorting by brand_name
        sales_data = db.session.query(
            SalesAllInOneLive.city,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.city,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.city).offset(offset).limit(limit).all()

        sales_datas1 = db.session.query(
            SalesAllInOneLive.city,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            case(
                (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
                else_=func.year(SalesAllInOneLive.invoice_date)
            ).label("fiscal_year"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("sales_qty"),
            func.sum(SalesAllInOneLive.tax_amt).label("tax_amt"),
            func.sum(SalesAllInOneLive.gros_profit).label("gros_profit"),
        ).filter(*conditions).group_by(
            SalesAllInOneLive.city,
            "fiscal_year",
            extract("month", SalesAllInOneLive.invoice_date),
        ).order_by(SalesAllInOneLive.city)

        sales_data1= sales_datas1.all()

        # Initialize result dict and year mapping
        result_dict = {}
        month_names = {
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            1: "Jan",
            2: "Feb",
            3: "Mar",
        }
        month_names1 = {
            "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9,
            "Oct":10, "Nov":11, "Dec":12, "Jan":1, "Feb":2, "Mar":3,
        }
        years_list = []
        yearly_totals = {}

        # Process sales data and organize into result_dict
        for city, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data:
            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            if total_sales is None:
                total_sales = 0
                sales_qty = 0
                tax_amt = 0
                gros_profit = 0

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                sales_details = sales_qty
            elif factor == 'total_sales':
                sales_details = total_sales
            elif factor == 'gp':
                sales_details = round(gros_profit / value, 2)
            else:
                if factor in ['cr', 'cr_without_gst']:
                    if factor == 'cr_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)
                elif factor in ['lk', 'lk_without_gst']:
                    if factor == 'lk_without_gst':
                        sales_details = round((total_sales - tax_amt) / value, 2)
                    else:
                        sales_details = round(total_sales / value, 2)

            sales_with_gst = sales_details

            if city not in result_dict:
                result_dict[city] = {}

            if fiscal_year not in result_dict[city]:
                result_dict[city][fiscal_year] = {}

            if city not in yearly_totals:
                yearly_totals[city] = {}

            if fiscal_year not in yearly_totals[city]:
                yearly_totals[city][fiscal_year] = 0

            yearly_totals[city][fiscal_year] += sales_with_gst

            result_dict[city][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        monthly_totals = {}
        for city, year, month,fiscal_year, total_sales, sales_qty, tax_amt, gros_profit in sales_data1:
            

            
            if total_sales is None:
                total_sales = 0
            if sales_qty is None:
                sales_qty = 0
            if tax_amt is None:
                tax_amt = 0
            if gros_profit is None:
                gros_profit = 0

            # Determine value based on gstfilter
            if factor == 'cr':
                value = 10000000
            elif factor == 'cr_without_gst':
                value = 10000000
            elif factor == 'lk':
                value = 100000
            elif factor == 'lk_without_gst':
                value = 100000
            elif factor == 'sales_qty':
                value = None  # Handle separately, as sales_qty doesn't need a factor
            elif factor == 'total_sales':
                value = None  # Handle separately
            elif factor == 'gp':
                value = 100000
            else:
                return jsonify({"success": 0, "error": f"Invalid factor: {factor}"}), 400

            # Calculate sales details based on the selected factor
            if factor == 'sales_qty':
                    sales_details = sales_qty
            elif factor == 'total_sales':
                    sales_details = total_sales
            elif factor == 'gp':
                    sales_details = gros_profit / value
            else:
                    if factor in ['cr', 'cr_without_gst']:
                        if factor == 'cr_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value
                    elif factor in ['lk', 'lk_without_gst']:
                        if factor == 'lk_without_gst':
                            sales_details = (total_sales - tax_amt) / value
                        else:
                            sales_details = total_sales / value

            sales_with_gst = Decimal(sales_details)

            if fiscal_year not in monthly_totals:
                    monthly_totals[fiscal_year] = {}
            if month not in monthly_totals[fiscal_year]:
                    monthly_totals[fiscal_year][month] = Decimal(0)

                # Add up sales for each month with full precision
            monthly_totals[fiscal_year][month] += sales_with_gst

            # Retrieve the specific monthly total
        # monthly_total1 = round(monthly_totals.get(2025, {}).get(4, Decimal(0)),2)
        # print(monthly_total1)

        # Calculate percentage of sales for each month and add to result_dict
        # for city, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         yearly_total = yearly_totals[city][fiscal_year]
        #         for month, data in months_data.items():
        #             if data["sales_with_gst"] == 0:
        #                 result_dict[city][fiscal_year][month] = f"{data['sales_with_gst']} ({0.00}%)"
        #             else:
        #                 percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
        #                 result_dict[city][fiscal_year][month] = f"{data['sales_with_gst']} ({percentage}%)"

        # monthly_totals = {}
        # for city, fiscal_year_data in result_dict.items():
        #     for fiscal_year, months_data in fiscal_year_data.items():
        #         for month, data in months_data.items():
        #             if fiscal_year not in monthly_totals:
        #                 monthly_totals[fiscal_year] = {}
                    
        #             if month not in monthly_totals[fiscal_year]:
        #                 monthly_totals[fiscal_year][month] = 0

        #             # Add up sales for each month
        #             monthly_totals[fiscal_year][month] = Decimal(monthly_totals[fiscal_year][month])
        #             monthly_totals[fiscal_year][month] += Decimal(data["sales_with_gst"])

        # Now, update the result_dict to calculate the percentage based on the monthly total for each fiscal year and month
        for city, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                for month, data in months_data.items():
                    financial_month = month_names1[month]
                    monthly_total = round(Decimal(monthly_totals[fiscal_year].get(financial_month, 0)),2)

                    sales_with_gst = round(Decimal(data['sales_with_gst']),2)
                    if monthly_total == 0:
                        result_dict[city][fiscal_year][month] = f"{sales_with_gst} ({0.00}%)"
                    else:
                        percentage = round((sales_with_gst / monthly_total) * 100, 2)
                        result_dict[city][fiscal_year][month] = f"{sales_with_gst} ({percentage}%)"

        years_list.reverse()

        
        
        # Return the result as JSON with pagination
        return jsonify({
            "years": years_list,
            "values": result_dict,
            "page": page,
            "limit": limit,
            "yearly_totals": yearly_totals,
        }), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_city_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def get_sales_all_in_one_live_table_modification_date_and_time_controller():
    try:
        # Query to get the latest update time from DATA_REFRESH_TIME column
        query = text("""
            SELECT MAX(DATA_REFRESH_TIME) AS Latest_Update 
            FROM apx_stock_apps.sales_all_in_one_live
        """)
        
        # Execute the query
        result = db.session.execute(query).fetchone()

        if result and result[0]:
            return jsonify({"success": 1, "last_modified": result[0]}), 200
        else:
            return jsonify({"success": 0, "error": "No modification date found."}), 400

    except Exception as e:
        db.session.rollback()
        return jsonify({"success": 0, "error": str(e)}), 500

