from collections import defaultdict
from flask import request, jsonify
from sqlalchemy import and_, case, extract, func
from src import db
import calendar
from datetime import datetime
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
def get_test_controller():
    try:
        sales_data = (
            db.session.query(
                func.date_format(SalesAllInOneLive.invoice_date, '%M').label("month"),
                extract("year", SalesAllInOneLive.invoice_date).label("year"),
                func.week(SalesAllInOneLive.invoice_date).label("week"),
                func.dayofweek(SalesAllInOneLive.invoice_date).label("day_of_week"),
                func.sum(SalesAllInOneLive.total_sales).label("total_sales")
            )
            .group_by(
                extract("year", SalesAllInOneLive.invoice_date),
                func.week(SalesAllInOneLive.invoice_date),
                func.dayofweek(SalesAllInOneLive.invoice_date)
            )
            .order_by(extract("year", SalesAllInOneLive.invoice_date).desc(),
                      func.week(SalesAllInOneLive.invoice_date).desc())
            .all()
        )

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
            week_key = f"Week {week+1}"
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
            return get_test_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

