from flask import request, jsonify
from sqlalchemy import and_, case, extract, func
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
from datetime import datetime
from collections import defaultdict
import traceback

# ----------------------------------------------------------------------------------------------------------
# ---------------------------------------- Main Methods ----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------


def get_sales_all_in_one_live_ytd_controller(factor):

    try:

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

        conditions = []
        previous_sales = None  

        for year in fiscal_years:
            start_date = datetime(year, start_month, 1)
            end_date = datetime(year, latest_month, latest_day)

            conditions = search_sales_all_in_one_controller()

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
        
        conditions = []
        conditions = search_sales_all_in_one_controller()

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

        conditions = []
        conditions = search_sales_all_in_one_controller()

        

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

        conditions = []
        conditions = search_sales_all_in_one_controller()
        
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

            # sales_with_gst = round(total_sales / 10000000, 2)
            grouped_sales[fiscal_year][month][week_key][day_key][
                "sales_details"
            ] = sales_details

            print(sales_details)
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

        for entry in formatted_data:
            print(entry)
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

        conditions = []
        conditions = search_sales_all_in_one_controller()

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

        conditions = []
        conditions = search_sales_all_in_one_controller()

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

        conditions = []
        conditions = search_sales_all_in_one_controller()

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

        conditions = []
        conditions = search_sales_all_in_one_controller()

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
        
        conditions = []
        conditions = search_sales_all_in_one_controller()

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
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')

        conditions = []

        if period_from and period_from != '':
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        
        if period_to and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)

        if invoice_date and invoice_date != '':
            conditions.append(SalesAllInOneLive.invoice_date == invoice_date)

        if srn_flag and srn_flag != '':
            conditions.append(SalesAllInOneLive.srn_flag == srn_flag)

        if sales_type and sales_type != '':
            conditions.append(SalesAllInOneLive.sale_type == sales_type)

        if section and section != '':
            conditions.append(SalesAllInOneLive.section == section)

        if brand_name and brand_name != '':
            conditions.append(SalesAllInOneLive.brand_name == brand_name)

        if model_no and model_no != '':
            conditions.append(SalesAllInOneLive.model_no == model_no)

        if item_description and item_description != '':
            conditions.append(SalesAllInOneLive.item_description == item_description)

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

        conditions = []
        previous_sales = None  # Track previous year's sales to calculate YoY percentage

        for year in fiscal_years:
            start_date = datetime(year, start_month, 1)
            end_date = datetime(year, latest_month, latest_day)

            conditions = search_sales_all_in_one_controller()

            # Query to calculate total sales for the YTD period
            total_sales = (
                db.session.query(func.sum(SalesAllInOneLive.total_sales))
                .filter(
                    SalesAllInOneLive.invoice_date >= start_date,
                    SalesAllInOneLive.invoice_date <= end_date,
                )
                .filter(*conditions)
                .scalar()
                or 0
            )

            sales_with_gst = round(total_sales / 10000000, 2)

            # Calculate YoY percentage change if previous year's sales data is available
            if previous_sales is not None:
                if previous_sales != 0:
                    percentage_change = round(
                        ((sales_with_gst - previous_sales) / previous_sales) * 100, 2
                    )
                    sales_with_gst_display = f"{sales_with_gst} ({'+' if percentage_change >= 0 else ''}{percentage_change}%)"
                else:
                    # Handle division by zero case for previous_sales
                    sales_with_gst_display = f"{sales_with_gst} (0.00%)"
            else:
                sales_with_gst_display = (
                    f"{sales_with_gst} (0.00%)"  # No previous year to compare
                )

            # Store result with YoY change
            result[year + 1] = sales_with_gst_display

            # Update previous_sales for next iteration
            previous_sales = sales_with_gst

        sorted_result = dict(sorted(result.items(), reverse=True))

        return jsonify(sorted_result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_ytd_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------------- Month ----------------------------------------


def get_sales_all_in_one_live_month_cr_controller():
    try:
        
        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
        ).filter(*conditions)

        sales_data = sales_data.group_by(
            extract("year", SalesAllInOneLive.invoice_date),
            extract("month", SalesAllInOneLive.invoice_date),
        ).all()

        print(sales_data)

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

        for year, month, total_sales in sales_data:
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]
            sales_with_gst = round(total_sales / 10000000, 2)

            previous_month_sales = previous_sales.get(fiscal_year, {}).get(
                month - 1 if month > 1 else 12
            )
            if previous_month_sales is not None:
                if previous_month_sales == 0:
                    previous_month_sales = 1
                percentage_change = round(
                    ((sales_with_gst - previous_month_sales) / previous_month_sales)
                    * 100,
                    2,
                )
                change_display = f"{sales_with_gst} ({'+' if percentage_change >= 0 else ''}{percentage_change}%)"
            else:
                change_display = (
                    f"{sales_with_gst} (0.00%)"  # No previous month for comparison
                )

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = {"FY": str(fiscal_year), "Total": 0}

            yearly_totals[fiscal_year][financial_month] = change_display
            yearly_totals[fiscal_year]["Total"] += sales_with_gst

            if fiscal_year not in previous_sales:
                previous_sales[fiscal_year] = {}
            previous_sales[fiscal_year][month] = sales_with_gst

        for year, data in yearly_totals.items():
            yearly_totals[year]["Total"] = round(yearly_totals[year]["Total"], 2)

        result_list = list(yearly_totals.values())

        return jsonify(result_list), 200

    except Exception as e:
        traceback.print_exc()
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------- Weekly Analysis ----------------------------------


def get_sales_all_in_one_live_weekly_analysis_cr_controller():
    try:
        
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

        conditions = []
        conditions = search_sales_all_in_one_controller()

        weekly_sales = db.session.query(
            week_number.label("week_number"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            func.round(func.sum(SalesAllInOneLive.total_sales) / 10000000, 2).label(
                "sales_with_gst"
            ),
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

        for week_number, month, year, sales_with_gst in weekly_sales:
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

            result_dict[week_label][fiscal_year] = {"sales_with_gst": sales_with_gst}

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = 0
            yearly_totals[fiscal_year] += sales_with_gst

        for week_label, year_data in result_dict.items():
            for fiscal_year, data in year_data.items():
                yearly_total = yearly_totals.get(fiscal_year, 0)
                if yearly_total > 0:
                    percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
                    result_dict[week_label][fiscal_year]["percentage"] = percentage

                    if data["sales_with_gst"] == 0:
                        result_dict[week_label][
                            fiscal_year
                        ] = f"{data['sales_with_gst']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_with_gst"] / yearly_total) * 100, 2
                        )
                        # result_dict[week_label][fiscal_year]["percentage"] = percentage
                        result_dict[week_label][
                            fiscal_year
                        ] = f"{data['sales_with_gst']} ({percentage}%)"

        years_list.sort(reverse=True)

        return jsonify({"values": result_dict, "years": years_list}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_weekly_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------- Day Analysis -------------------------------------


def get_sales_all_in_one_live_day_analysis_cr_controller():
    try:

        conditions = []
        conditions = search_sales_all_in_one_controller()
        
        sales_data = db.session.query(
            func.date_format(SalesAllInOneLive.invoice_date, "%M").label("month"),
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            func.week(SalesAllInOneLive.invoice_date).label("week"),
            func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
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
                        "mon": {"sales_with_gst": "-", "percentage": "-"},
                        "tue": {"sales_with_gst": "-", "percentage": "-"},
                        "wed": {"sales_with_gst": "-", "percentage": "-"},
                        "thu": {"sales_with_gst": "-", "percentage": "-"},
                        "fri": {"sales_with_gst": "-", "percentage": "-"},
                        "sat": {"sales_with_gst": "-", "percentage": "-"},
                        "sun": {"sales_with_gst": "-", "percentage": "-"},
                    }
                )
            )
        )

        weekly_totals = defaultdict(lambda: defaultdict(float))

        for month, year, week, day_of_week, total_sales in sales_data:
            fiscal_year = year if month in [1, 2, 3] else year + 1
            day_key = day_mapping[day_of_week]
            week_key = f"Week{week}"

            sales_with_gst = round(total_sales / 10000000, 2)
            grouped_sales[fiscal_year][month][week_key][day_key][
                "sales_with_gst"
            ] = sales_with_gst

            weekly_totals[fiscal_year][week_key] += sales_with_gst

        for fiscal_year, months in grouped_sales.items():
            for month, weeks in months.items():
                for week, sales in weeks.items():
                    weekly_total = weekly_totals[fiscal_year][week]
                    if weekly_total > 0:
                        for day in day_mapping.values():
                            if sales[day]["sales_with_gst"] != "-":
                                percentage = round(
                                    (sales[day]["sales_with_gst"] / weekly_total) * 100,
                                    2,
                                )
                                sales[day] = (
                                    f"{sales[day]['sales_with_gst']} ({percentage}%)"
                                )
                                # sales[day]["percentage"] = percentage
                            else:
                                sales[day] = f"{0.00} ({0.00}%)"

                    sales["year"] = str(fiscal_year)
                    sales["month"] = month
                    sales["week"] = week

                    formatted_data.append(sales)

        for entry in formatted_data:
            print(entry)
        return jsonify(formatted_data), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_day_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------- Product Dimension ----------------------------------


def get_sales_all_in_one_live_product_dimension_cr_controller():
    try:

        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            SalesAllInOneLive.product_group,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
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

        for product_group, year, month, total_sales in sales_data:

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)

            if product_group not in result_dict:
                result_dict[product_group] = {}

            if fiscal_year not in result_dict[product_group]:
                result_dict[product_group][fiscal_year] = {}

            if product_group not in yearly_totals:
                yearly_totals[product_group] = {}

            if fiscal_year not in yearly_totals[product_group]:
                yearly_totals[product_group][fiscal_year] = 0

            # result_dict[product_group][fiscal_year][financial_month] = sales_with_gst

            yearly_totals[product_group][fiscal_year] += sales_with_gst

            result_dict[product_group][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        for product_group, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[product_group][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():
                    if data["sales_with_gst"] == 0:
                        result_dict[product_group][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_with_gst"] / yearly_total) * 100, 2
                        )
                        # result_dict[product_group][fiscal_year][month]["percentage"] = percentage
                        result_dict[product_group][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_product_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ------------------------------------- Brand Dimension --------------------------------------


def get_sales_all_in_one_live_brand_dimension_cr_controller():
    try:

        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            SalesAllInOneLive.brand_name,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
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

        for brand_name, year, month, total_sales in sales_data:

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)

            if brand_name not in result_dict:
                result_dict[brand_name] = {}

            if fiscal_year not in result_dict[brand_name]:
                result_dict[brand_name][fiscal_year] = {}

            if brand_name not in yearly_totals:
                yearly_totals[brand_name] = {}

            if fiscal_year not in yearly_totals[brand_name]:
                yearly_totals[brand_name][fiscal_year] = 0

            # result_dict[brand_name][fiscal_year][financial_month] = sales_with_gst

            yearly_totals[brand_name][fiscal_year] += sales_with_gst

            result_dict[brand_name][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }

        for brand_name, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[brand_name][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():
                    if data["sales_with_gst"] == 0:
                        result_dict[brand_name][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_with_gst"] / yearly_total) * 100, 2
                        )
                        # result_dict[brand_name][fiscal_year][month]["percentage"] = percentage
                        result_dict[brand_name][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list, "values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_brand_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -------------------------------------- Item Dimension --------------------------------------


def get_sales_all_in_one_live_item_dimension_cr_controller():
    try:

        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            SalesAllInOneLive.actual_item,
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
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

        for actual_item, year, month, total_sales in sales_data:
            if actual_item is None or actual_item == "":
                actual_item="Emp"  # Skip this iteration if any value is None
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            total_sales = float(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)

            if actual_item not in result_dict:
                result_dict[actual_item] = {}

            if fiscal_year not in result_dict[actual_item]:
                result_dict[actual_item][fiscal_year] = {}

            if actual_item not in yearly_totals:
                yearly_totals[actual_item] = {}

            if fiscal_year not in yearly_totals[actual_item]:
                yearly_totals[actual_item][fiscal_year] = 0

            # result_dict[actual_item][fiscal_year][financial_month] = sales_with_gst

            yearly_totals[actual_item][fiscal_year] += sales_with_gst

            result_dict[actual_item][fiscal_year][financial_month] = {
                "sales_with_gst": sales_with_gst,
            }
        max_sales_with_gst = 0
        for actual_item, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[actual_item][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():

                    data_sales_with_gst = data["sales_with_gst"]

                    if data_sales_with_gst > max_sales_with_gst:
                        max_sales_with_gst = data_sales_with_gst

                    if data["sales_with_gst"] == 0:
                        result_dict[actual_item][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({0.00}%)"
                    else:
                        percentage = round(
                            (data["sales_with_gst"] / yearly_total) * 100, 2
                        )
                        # result_dict[actual_item][fiscal_year][month]["percentage"] = percentage
                        result_dict[actual_item][fiscal_year][
                            month
                        ] = f"{data['sales_with_gst']} ({percentage}%)"

        years_list.reverse()
        print({"years": years_list, "values": result_dict, "max": max_sales_with_gst})
        return jsonify({"years": years_list, "values": result_dict, "max": max_sales_with_gst}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------- Price Breakup 1 ------------------------------------


def get_sales_all_in_one_live_price_breakup_one_cr_controller():
    try:

        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
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

        for year, month, total_sales, total_qty in sales_data:
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]
            years_set.add(fiscal_year)

            piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
            sales_with_gst = round(total_sales / 10000000, 2)

            total_sales_by_year[fiscal_year] += sales_with_gst

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
                price_ranges[price_breakup][fiscal_year] += sales_with_gst

        years_list = sorted(years_set, reverse=True)

        for price_range, sales_data in price_ranges.items():
            for year in years_list:
                sales_value = sales_data.get(year, 0)
                total_sales = total_sales_by_year[year]
                percentage = (
                    round((sales_value / total_sales) * 100, 2)
                    if total_sales > 0
                    else 0
                )
                # sales_data[year] = {
                #     "sales_with_gst": sales_value,
                #     "percentage": percentage
                # }

                sales_data[year] = f"{sales_value} ({percentage}%)"

        return jsonify({"years": years_list, "values": price_ranges}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------- Price Breakup 2 ------------------------------------


def get_sales_all_in_one_live_price_breakup_two_cr_controller():
    try:
        
        conditions = []
        conditions = search_sales_all_in_one_controller()

        sales_data = db.session.query(
            extract("year", SalesAllInOneLive.invoice_date).label("year"),
            extract("month", SalesAllInOneLive.invoice_date).label("month"),
            func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            func.sum(SalesAllInOneLive.sales_qty).label("total_qty"),
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

        for year, month, total_sales, total_qty in sales_data:
            piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
            sales_with_gst = round(total_sales / 10000000, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            years_set.add(fiscal_year)

            total_sales_by_year[fiscal_year] += sales_with_gst

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
                price_ranges[price_breakup][fiscal_year] += sales_with_gst

        years_list = sorted(years_set, reverse=True)

        for price_range, sales_data in price_ranges.items():
            for year in years_list:
                sales_value = sales_data.get(year, 0)
                total_sales = total_sales_by_year[year]
                percentage = (
                    round((sales_value / total_sales) * 100, 2)
                    if total_sales > 0
                    else 0
                )
                # sales_data[year] = {
                #     "sales_with_gst": sales_value,
                #     "percentage": percentage
                # }

                sales_data[year] = f"{sales_value} ({percentage}%)"

        return jsonify({"years": years_list, "values": price_ranges}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_two_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

