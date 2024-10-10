from flask import request, jsonify
from sqlalchemy import and_, case, extract, func
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
from datetime import datetime
from collections import defaultdict

# ----------------------------------------------------------------------------------------------------------
# ---------------------------------------- Main Methods ----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------

def get_sales_all_in_one_live_ytd_controller(factor):
    pass


def get_sales_all_in_one_live_monthly_controller(factor):
    pass


def get_sales_all_in_one_live_weekly_analysis_controller(factor):
    pass

def get_sales_all_in_one_live_day_analysis_controller(factor):
    pass


def get_sales_all_in_one_live_product_dimension_controller(factor):
    pass


def get_sales_all_in_one_live_brand_dimension_controller(factor):
    pass


def get_sales_all_in_one_live_item_dimension_controller(factor):
    pass


def get_sales_all_in_one_live_price_breakup_one_controller(factor):
    pass


def get_sales_all_in_one_live_price_breakup_two_controller(factor):
    pass



# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------- Main Methods (END) ---------------------------------------------
# ----------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------- Utility Functions  ----------------------------------------------
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



# ----------------------------------------------------------------------------------------------------------
# ------------------------------------------------- Cr -----------------------------------------------------
# ----------------------------------------------------------------------------------------------------------

# -------------------------------------------- YTD -------------------------------------------


def get_sales_all_in_one_live_ytd_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        latest_invoice_date = (
            db.session.query(func.max(SalesAllInOneLive.invoice_date))
            .scalar()
        )

        if not latest_invoice_date:
            return jsonify({"success": 0, "error": "No sales data found."}), 404

        latest_year = latest_invoice_date.year
        latest_month = latest_invoice_date.month
        latest_day = latest_invoice_date.day

        start_month = 4 

        fiscal_years = [latest_year, latest_year - 1, latest_year - 2, latest_year - 3]

        result = {}

        
        for year in fiscal_years:
            start_date = datetime(year, start_month, 1)
            end_date = datetime(year, latest_month, latest_day)

            if year != latest_year:
                end_date = datetime(year, latest_month, latest_day)

            total_sales = (
                db.session.query(func.sum(SalesAllInOneLive.total_sales))
                .filter(
                    SalesAllInOneLive.invoice_date >= start_date,
                    SalesAllInOneLive.invoice_date <= end_date
                )
                .scalar() or 0  
            )

            print(total_sales)
            sales_with_gst = round(total_sales / 10000000, 2)
            result[year + 1] = sales_with_gst

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

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

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

       
        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)


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

        for year, month, total_sales in sales_data:
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]
            sales_with_gst = round(total_sales / 10000000, 2)

            if fiscal_year not in yearly_totals:
                yearly_totals[fiscal_year] = {"FY": str(fiscal_year), "Total": 0}

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


# ----------------------------------------- Weekly Analysis ----------------------------------


def get_sales_all_in_one_live_weekly_analysis_cr_controller1():
    try:
        fiscal_start_month = 4
        fiscal_start_day = 1

        fiscal_start_date = func.concat(
            func.year(SalesAllInOneLive.invoice_date) - case(
                (extract('month', SalesAllInOneLive.invoice_date) < fiscal_start_month, 1),
                else_=0
            ),
            '-',
            fiscal_start_month,
            '-',
            fiscal_start_day
        )
        
        week_number = func.floor(func.datediff(SalesAllInOneLive.invoice_date, fiscal_start_date) / 7) + 1

        weekly_sales = (
            db.session.query(
                week_number.label("week_number"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                func.round(func.sum(SalesAllInOneLive.total_sales) / 10000000, 2).label("sales_with_gst")
            )
            .group_by(week_number)
            .order_by(week_number)
            .all()
        )

        # result = [
        #     {
        #         "week_number": row.week_number,
        #         "year": row.year,
        #         "sales_with_gst": row.sales_with_gst,
        #         "month": row.month
        #     }
        #     for row in weekly_sales
        # ]


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

        result = []   
        result_dict = {}
        years_list = []


        for week_number, month, year, sales_with_gst in weekly_sales:
            
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            if fiscal_year not in result_dict:
                result_dict[fiscal_year] = {}

            result_dict[fiscal_year][week_number] = sales_with_gst

    
        # years_list.reverse()
        return jsonify({"years": years_list,"values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_weekly_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})



def get_sales_all_in_one_live_weekly_analysis_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)
        
        
        fiscal_start_month = 4
        fiscal_start_day = 1

        fiscal_start_date = func.concat(
            func.year(SalesAllInOneLive.invoice_date) - case(
                (extract('month', SalesAllInOneLive.invoice_date) < fiscal_start_month, 1),
                else_=0
            ),
            '-',
            fiscal_start_month,
            '-',
            fiscal_start_day
        )

        week_number = func.floor(func.datediff(SalesAllInOneLive.invoice_date, fiscal_start_date) / 7) + 1

        weekly_sales = (
            db.session.query(
                week_number.label("week_number"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                func.round(func.sum(SalesAllInOneLive.total_sales) / 10000000, 2).label("sales_with_gst")
            )
            
        )

        if period_from:
            weekly_sales = weekly_sales.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            weekly_sales = weekly_sales.filter(SalesAllInOneLive.invoice_date <= period_to)


        weekly_sales = weekly_sales.group_by(week_number, extract("year", SalesAllInOneLive.invoice_date), extract("month", SalesAllInOneLive.invoice_date)).order_by(week_number).all()

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

        result_dict = {}  # To store final output
        years_list = []   # To store years seen in the data

        for week_number, month, year, sales_with_gst in weekly_sales:
            # Only include week numbers 1 to 52
            if week_number > 52:
                continue

            # Determine the fiscal year
            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            # Keep track of all fiscal years
            if fiscal_year not in years_list:
                years_list.append(fiscal_year)

            # Format the week number (e.g., "Week 01", "Week 02")
            week_label = f"Week {int(week_number):02}"

            # Initialize the week entry if not already present
            if week_label not in result_dict:
                result_dict[week_label] = {}

            # Assign sales data to the correct fiscal year under each week
            result_dict[week_label][fiscal_year] = sales_with_gst

        # Sort the years in reverse order if needed
        years_list.sort(reverse=True)

        return jsonify({"values": result_dict, "years": years_list}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_weekly_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# ----------------------------------------- Day Analysis -------------------------------------


def get_sales_all_in_one_live_day_analysis_cr_controller1():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)
        sales_data = (
            db.session.query(
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                func.week(SalesAllInOneLive.invoice_date).label("week"),
                func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales")
            )
            
        )


        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

        sales_data = sales_data.group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                func.week(SalesAllInOneLive.invoice_date),
                func.dayofweek(SalesAllInOneLive.invoice_date)
            ).order_by(extract("year", SalesAllInOneLive.invoice_date).desc()).all()

        day_map = {
            1: "Mon",
            2: "Tue",
            3: "Wed",
            4: "Thu",
            5: "Fri",
            6: "Sat",
            7: "Sun"
        }

        result = {}
        for month, year, week, day_of_week, total_sales in sales_data:
            fiscal_year = year if month in [1, 2, 3] else year + 1
            
            sales_with_gst = round(total_sales / 10000000, 2)

            if fiscal_year not in result:
                result[fiscal_year] = {}
            
            if week not in result[fiscal_year]:
                result[fiscal_year][week] = {day: None for day in day_map.values()}

            result[fiscal_year][week][day_map[day_of_week]] = sales_with_gst

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_day_analysis_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_sales_all_in_one_live_day_analysis_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                func.date_format(SalesAllInOneLive.invoice_date, '%M').label("month"),
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                func.week(SalesAllInOneLive.invoice_date).label("week"),
                func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales")
            )
            
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

        sales_data = sales_data.group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                func.week(SalesAllInOneLive.invoice_date),
                func.dayofweek(SalesAllInOneLive.invoice_date)
            ).order_by(extract("year", SalesAllInOneLive.invoice_date).desc()).all()

        # Prepare the structured output
        formatted_data = []

        # Create a mapping for the weekdays
        day_mapping = {
            1: "mon",
            2: "tue",
            3: "wed",
            4: "thu",
            5: "fri",
            6: "sat",
            7: "sun"
        }

        # Create a nested dictionary to hold the data grouped by year, month, and week
        grouped_sales = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
            "year": None,
            "month": None,
            "week": None,
            "mon": "-",
            "tue": "-",
            "wed": "-",
            "thu": "-",
            "fri": "-",
            "sat": "-",
            "sun": "-",
        })))

        # Populate the grouped_sales dictionary
        for month, year, week, day_of_week, total_sales in sales_data:
            fiscal_year = year if month in [1, 2, 3] else year + 1
            day_key = day_mapping[day_of_week]
            week_key = f"Week{week}"
            grouped_sales[fiscal_year][month][week_key][day_key] = round(total_sales / 10000000, 2)

        # Format the output
        for fiscal_year, months in grouped_sales.items():
            for month, weeks in months.items():
                for week, sales in weeks.items():
                    sales["year"] = str(fiscal_year)
                    sales["month"] = month
                    sales["week"] = week
                    formatted_data.append(sales)

        # Display the formatted data
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

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                SalesAllInOneLive.product_group,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

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
                        result_dict[product_group][fiscal_year][month] = f"{data["sales_with_gst"]} ({0.00}%)"
                    else:
                        percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
                    # result_dict[product_group][fiscal_year][month]["percentage"] = percentage
                        result_dict[product_group][fiscal_year][month] = f"{data["sales_with_gst"]} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list,"values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_product_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ------------------------------------- Brand Dimension --------------------------------------


def get_sales_all_in_one_live_brand_dimension_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                SalesAllInOneLive.brand_name,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)


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
                        result_dict[brand_name][fiscal_year][month] = f"{data["sales_with_gst"]} ({0.00}%)"
                    else:
                        percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
                    # result_dict[brand_name][fiscal_year][month]["percentage"] = percentage
                        result_dict[brand_name][fiscal_year][month] = f"{data["sales_with_gst"]} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list,"values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_brand_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -------------------------------------- Item Dimension --------------------------------------


def get_sales_all_in_one_live_item_dimension_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                SalesAllInOneLive.actual_item,
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
            )
            
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

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

        for actual_item, fiscal_year_data in result_dict.items():
            for fiscal_year, months_data in fiscal_year_data.items():
                yearly_total = yearly_totals[actual_item][fiscal_year]
                # if yearly_total > 0:
                for month, data in months_data.items():
                    if data["sales_with_gst"] == 0:
                        result_dict[actual_item][fiscal_year][month] = f"{data["sales_with_gst"]} ({0.00}%)"
                    else:
                        percentage = round((data["sales_with_gst"] / yearly_total) * 100, 2)
                    # result_dict[actual_item][fiscal_year][month]["percentage"] = percentage
                        result_dict[actual_item][fiscal_year][month] = f"{data["sales_with_gst"]} ({percentage}%)"

        years_list.reverse()
        return jsonify({"years": years_list,"values": result_dict}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_item_dimension_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------- Price Breakup 1 ------------------------------------


def get_sales_all_in_one_live_price_breakup_one_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
                func.sum(SalesAllInOneLive.sales_qty).label("total_qty")
            )
            
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

        sales_data = sales_data.group_by(extract("year", SalesAllInOneLive.invoice_date)).all()

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
            else:
                price_breakup = "Null"

            if price_breakup != "Null":
                price_ranges[price_breakup][fiscal_year] = sales_with_gst

        # result_dict = {k: v for k, v in price_ranges.items()}

        years_list = sorted(years_set, reverse=True)
        for price_range in price_ranges:
            for year in years_list:
                price_ranges[price_range].setdefault(year, 0)

        return jsonify({"years": years_list,"values": price_ranges}), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_one_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# --------------------------------------- Price Breakup 2 ------------------------------------


def get_sales_all_in_one_live_price_breakup_two_cr_controller():
    try:

        period_from = request.args.get('period_from')
        period_to = request.args.get('period_to')

        print("------------>", period_from, period_to)

        sales_data = (
            db.session.query(
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                extract("month", SalesAllInOneLive.invoice_date).label("month"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales"),
                func.sum(SalesAllInOneLive.sales_qty).label("total_qty")
            )
            
        )

        if period_from:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            sales_data = sales_data.filter(SalesAllInOneLive.invoice_date <= period_to)

        sales_data = sales_data.group_by(extract("year", SalesAllInOneLive.invoice_date)).all()

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

        years_set = set()

        for year, month, total_sales, total_qty in sales_data:
            piecewise_sales = total_sales / total_qty if total_qty > 0 else 0
            sales_with_gst = round(total_sales / 10000000, 2)

            if month in [1, 2, 3]:
                fiscal_year = year
            else:
                fiscal_year = year + 1

            financial_month = month_names[month]

            years_set.add(fiscal_year)

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
                price_ranges[price_breakup][fiscal_year] = sales_with_gst

        # result_dict = {k: v for k, v in price_ranges.items()}

        years_list = sorted(years_set, reverse=True)
        for price_range in price_ranges:
            for year in years_list:
                price_ranges[price_range].setdefault(year, 0)

        return jsonify({"years": years_list,"values": price_ranges}), 200


    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_price_breakup_two_cr_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------
# ------------------------------------------------ CR (END) ------------------------------------------------
# ----------------------------------------------------------------------------------------------------------





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

            result_dict[year].append(
                {"month": financial_month, "Sales without GST (Cr)": sales_without_gst}
            )

            yearly_totals[year] += sales_without_gst

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

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

            result_dict[year].append(
                {
                    "month": financial_month,
                    "Sales with GST (Lk)": sales_lk,
                }
            )

            yearly_totals[year] += sales_lk

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

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

            result_dict[year].append(
                {
                    "month": financial_month,
                    "Sales without GST (Lk)": sales_without_gst_lk,
                }
            )

            yearly_totals[year] += sales_without_gst_lk

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

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

            result_dict[year].append({"month": financial_month, "Sales Qty": sales_qty})

            yearly_totals[year] += sales_qty

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

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

            result_dict[year].append(
                {"month": financial_month, "Total Sales": total_sales}
            )

            yearly_totals[year] += total_sales

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

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

            result_dict[year].append(
                {"month": financial_month, "GP (Lk)": gros_profit_lk}
            )

            yearly_totals[year] += gros_profit_lk

        for year, month_data in result_dict.items():
            month_data.append({"total": round(yearly_totals[year], 2)})

        result = [{str(year): month_data} for year, month_data in result_dict.items()]

        return jsonify(result), 200

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_sales_all_in_one_live_month_gp_lk_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------

