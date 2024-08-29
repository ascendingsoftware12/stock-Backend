from flask import request, jsonify
from sqlalchemy import and_, func
from src import db
from src.controllers.category_controller import get_section_controller
from src.models.excess_stock_model import ExcessStockModel


# -----------------------------------------------------
def get_overall_stock_position_controller():

    try:
        base_query = db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
            ExcessStockModel.SECTION == "MOBILE"
        )
        never_sold_total = (
            base_query.filter(ExcessStockModel.IT_FLAG == "NEVER SOLD").scalar()
        ) or 0

        not_sold_total = (
            base_query.filter(ExcessStockModel.IT_FLAG == "NOT SOLD").scalar()
        ) or 0

        saleable_total = (
            base_query.filter(ExcessStockModel.IT_FLAG == "SALEABLE").scalar()
        ) or 0

        overall_stock_position_total = stock_position__total(
            never_sold_total, not_sold_total, saleable_total
        )
        return jsonify(overall_stock_position_total)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_stock_position_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_stock_position_controller():
    try:
        base_query = db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
            ExcessStockModel.SECTION == "MOBILE"
        )

        never_sold_total = (
            base_query.filter(ExcessStockModel.ST_IT_FLAG == "NEVER SOLD").scalar()
        ) or 0

        not_sold_total = (
            base_query.filter(ExcessStockModel.ST_IT_FLAG == "NOT SOLD").scalar()
        ) or 0

        saleable_total = (
            base_query.filter(ExcessStockModel.ST_IT_FLAG == "SALEABLE").scalar()
        ) or 0

        shop_level_stock_position_total = stock_position__total(
            never_sold_total, not_sold_total, saleable_total
        )
        return jsonify(shop_level_stock_position_total)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_stock_position_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


# -----------------------------------------------------


def get_overall_new_and_stock_position_controller():
    try:
        never_sold_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.IT_FLAG == "NEVER SOLD",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        never_sold_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.IT_FLAG == "NEVER SOLD",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        not_sold_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.IT_FLAG == "NOT SOLD",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        not_sold_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.IT_FLAG == "NOT SOLD",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        saleable_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.IT_FLAG == "SALEABLE",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        saleable_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            ) 
            .filter(
                ExcessStockModel.IT_FLAG == "SALEABLE",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        overall_new_and_stock_position_total = new_and_stock_position__total(
            never_sold_old_stock_total,
            never_sold_new_stock_total,
            not_sold_old_stock_total,
            not_sold_new_stock_total,
            saleable_old_stock_total,
            saleable_new_stock_total,
        )

        return jsonify(overall_new_and_stock_position_total)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_new_and_stock_position_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_new_and_stock_position_controller():
    try:
        never_sold_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "NEVER SOLD",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        never_sold_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "NEVER SOLD",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        not_sold_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "NOT SOLD",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        not_sold_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "NOT SOLD",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        saleable_old_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                ExcessStockModel.OVERALL_AGE > 30,
            )
            .scalar()
        ) or 0

        saleable_new_stock_total = (
            db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
            .filter(
                ExcessStockModel.SECTION == "MOBILE"
            )
            .filter(
                ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                ExcessStockModel.OVERALL_AGE <= 30,
            )
            .scalar()
        ) or 0

        shop_level_new_and_stock_position_total = new_and_stock_position__total(
            never_sold_old_stock_total,
            never_sold_new_stock_total,
            not_sold_old_stock_total,
            not_sold_new_stock_total,
            saleable_old_stock_total,
            saleable_new_stock_total,
        )

        return jsonify(shop_level_new_and_stock_position_total)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_new_and_stock_position_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


# -----------------------------------------------------
def get_overall_stock_ageing_controller():
    try:
        results = {
            "never_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "not_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "saleable": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "overall": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
        }

        category_mapping = {
            "NEVER SOLD": "never_sold",
            "NOT SOLD": "not_sold",
            "SALEABLE": "saleable",
        }

        def calculate_stock(category, min_age, max_age):
            query = db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
            ExcessStockModel.SECTION == "MOBILE"
            ).filter(
                ExcessStockModel.IT_FLAG == category,
                ExcessStockModel.OVERALL_AGE > min_age,
            )
            if max_age is not None:
                query = query.filter(ExcessStockModel.OVERALL_AGE <= max_age)
            return query.scalar() or 0

        for age_range in ["0to30", "30to60", "60to90", "90to180", "above_180"]:
            min_age, max_age = get_age_range_bounds(age_range)

            for category in ["NEVER SOLD", "NOT SOLD", "SALEABLE"]:
                stock_sum = calculate_stock(category, min_age, max_age)
                category_key = category_mapping[category]
                results[category_key][f"{age_range}_stock"] = stock_sum
                results[category_key]["total_stock"] += stock_sum

                results["overall"][f"{age_range}_stock"] += stock_sum

        results["overall"]["total_stock"] = (
            results["never_sold"]["total_stock"]
            + results["not_sold"]["total_stock"]
            + results["saleable"]["total_stock"]
        )

        def calculate_percentage(value, total):
            return (value / total) * 100 if total > 0 else 0

        for category in ["never_sold", "not_sold", "saleable", "overall"]:
            total_stock = results[category]["total_stock"]
            for age_range in ["0to30", "30to60", "60to90", "90to180", "above_180"]:
                stock_value = results[category][f"{age_range}_stock"]
                results[category][f"{age_range}_stock_percentage"] = (
                    calculate_percentage(stock_value, total_stock)
                )
            results[category]["total_stock_percentage"] = calculate_percentage(
                total_stock, results["overall"]["total_stock"]
            )

        return jsonify(results), 201

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_stock_ageing_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_stock_ageing_controller():
    try:
        results = {
            "never_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "not_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "saleable": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "overall": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
        }

        category_mapping = {
            "NEVER SOLD": "never_sold",
            "NOT SOLD": "not_sold",
            "SALEABLE": "saleable",
        }

        def calculate_stock(category, min_age, max_age):
            query = db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
                ExcessStockModel.SECTION == "MOBILE"
            ).filter(
                ExcessStockModel.ST_IT_FLAG == category,
                ExcessStockModel.OVERALL_AGE > min_age,
            )
            if max_age is not None:
                query = query.filter(ExcessStockModel.OVERALL_AGE <= max_age)
            return query.scalar() or 0

        for age_range in ["0to30", "30to60", "60to90", "90to180", "above_180"]:
            min_age, max_age = get_age_range_bounds(age_range)

            for category in ["NEVER SOLD", "NOT SOLD", "SALEABLE"]:
                stock_sum = calculate_stock(category, min_age, max_age)
                category_key = category_mapping[category]
                results[category_key][f"{age_range}_stock"] = stock_sum
                results[category_key]["total_stock"] += stock_sum

                results["overall"][f"{age_range}_stock"] += stock_sum

        results["overall"]["total_stock"] = (
            results["never_sold"]["total_stock"]
            + results["not_sold"]["total_stock"]
            + results["saleable"]["total_stock"]
        )

        def calculate_percentage(value, total):
            return (value / total) * 100 if total > 0 else 0

        for category in ["never_sold", "not_sold", "saleable", "overall"]:
            total_stock = results[category]["total_stock"]
            for age_range in ["0to30", "30to60", "60to90", "90to180", "above_180"]:
                stock_value = results[category][f"{age_range}_stock"]
                results[category][f"{age_range}_stock_percentage"] = (
                    calculate_percentage(stock_value, total_stock)
                )
            results[category]["total_stock_percentage"] = calculate_percentage(
                total_stock, results["overall"]["total_stock"]
            )

        return jsonify(results), 201

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_stock_ageing_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_age_range_bounds(age_range):
    if age_range == "0to30":
        return 0, 30
    elif age_range == "30to60":
        return 30, 60
    elif age_range == "60to90":
        return 60, 90
    elif age_range == "90to180":
        return 90, 180
    elif age_range == "above_180":
        return 180, None
    else:
        raise ValueError("Invalid age range")


# -----------------------------------------------------
def get_overall_sales_category_by_stock_qty_bucket_controller():
    try:
        pieces_range = [
            "1-5 Pieces",
            "6-10 Pieces",
            "11-15 Pieces",
            "16-20 Pieces",
            "21-25 Pieces",
            ">25 Pieces",
        ]
        results = {"NEVER SOLD": {}, "SOLD WITHIN 1 MONTH": {}, "NOT SOLD > 1 MONTH": {}, "TOTAL": {}}
        grand_totals = {"NEVER SOLD": 0, "SOLD WITHIN 1 MONTH": 0, "NOT SOLD > 1 MONTH": 0, "TOTAL": 0}

        for range_value in pieces_range:
            total_stock_sum = {
                "NEVER SOLD": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.IT_FLAG == "NEVER SOLD",
                        ExcessStockModel.IT_NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
                "SOLD WITHIN 1 MONTH": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.IT_FLAG == "SALEABLE",
                        ExcessStockModel.IT_NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
                "NOT SOLD > 1 MONTH": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK)).filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.IT_FLAG == "NOT SOLD",
                        ExcessStockModel.IT_NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
            }

            for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]:
                results[flag][range_value] = total_stock_sum[flag]
                grand_totals[flag] += total_stock_sum[flag]

            results["TOTAL"][range_value] = sum(total_stock_sum.values())
            grand_totals["TOTAL"] += results["TOTAL"][range_value]

        results["grand_totals"] = grand_totals

        return jsonify(results)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_sales_category_by_stock_qty_bucket_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_sales_category_by_stock_qty_bucket_controller():
    try:
        pieces_range = [
            "1-2 Pieces",
            "3-4 Pieces",
            "5-6 Pieces",
            "7-8 Pieces",
            "9-10 Pieces",
            ">10 Pieces",
        ]

        results = {"NEVER SOLD": {}, "SOLD WITHIN 1 MONTH": {}, "NOT SOLD > 1 MONTH": {}, "TOTAL": {}}
        grand_totals = {"NEVER SOLD": 0, "SOLD WITHIN 1 MONTH": 0, "NOT SOLD > 1 MONTH": 0, "TOTAL": 0}

        for range_value in pieces_range:
            total_stock_sum = {
                "NEVER SOLD": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                    .filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.ST_IT_FLAG == "NEVER SOLD",
                        ExcessStockModel.NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
                "SOLD WITHIN 1 MONTH": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                    .filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                        ExcessStockModel.NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
                "NOT SOLD > 1 MONTH": (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                    .filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.ST_IT_FLAG == "NOT SOLD",
                        ExcessStockModel.NO_OF_PIECES == range_value,
                    )
                    .scalar()
                )
                or 0,
            }

            for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]:
                results[flag][range_value] = total_stock_sum[flag]
                grand_totals[flag] += total_stock_sum[flag]

            results["TOTAL"][range_value] = sum(total_stock_sum.values())
            grand_totals["TOTAL"] += results["TOTAL"][range_value]

        results["grand_totals"] = grand_totals

        return jsonify(results)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_sales_category_by_stock_qty_bucket_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


# -----------------------------------------------------
def get_overall_dsi_by_saleable_qty_bucket_controller():
    try:
        pieces_range = [
            "1-5 Pieces",
            "6-10 Pieces",
            "11-15 Pieces",
            "16-20 Pieces",
            "21-25 Pieces",
            ">25 Pieces",
        ]

        dsi_range = [
            "1-7 Days",
            "8-14 Days",
            "15-30 Days",
            "31-60 Days",
            "61-90 Days",
            ">90 Days",
        ]

        results = {
            "pieces_range_totals": {},
            "dsi_range_totals": {},
            "combined_totals": {},
            "grand_totals": {"pieces_range": 0, "dsi_range": 0, "overall_total": 0},
        }

        for piece_range in pieces_range:
            total_stock_sum = (
                db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                )
                .filter(
                    ExcessStockModel.IT_FLAG == "SALEABLE",
                    ExcessStockModel.SALES_IT_NO_OF_PIECES == piece_range,
                )
                .scalar()
            ) or 0
            results["pieces_range_totals"][piece_range] = total_stock_sum
            results["grand_totals"]["pieces_range"] += total_stock_sum

        for dsi in dsi_range:
            total_stock_sum = (
                db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                )
                .filter(
                    ExcessStockModel.IT_FLAG == "SALEABLE",
                    ExcessStockModel.IT_DSI == dsi,
                )
                .scalar()
            ) or 0
            results["dsi_range_totals"][dsi] = total_stock_sum
            results["grand_totals"]["dsi_range"] += total_stock_sum

        combined_totals = {}
        grand_totals_combined = 0
        for piece_range in pieces_range:
            for dsi in dsi_range:
                total_stock_sum = (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                    .filter(
                        ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.IT_FLAG == "SALEABLE",
                        ExcessStockModel.SALES_IT_NO_OF_PIECES == piece_range,
                        ExcessStockModel.IT_DSI == dsi,
                    )
                    .scalar()
                ) or 0

                key = f"{piece_range} - {dsi}"
                combined_totals[key] = total_stock_sum
                grand_totals_combined += total_stock_sum

        results["combined_totals"] = combined_totals
        results["grand_totals"]["overall_total"] = grand_totals_combined

        return jsonify(results)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_dsi_by_saleable_qty_bucket_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_dsi_by_saleable_qty_bucket_controller():
    try:
        pieces_range = [
            "1-2 Pieces",
            "3-4 Pieces",
            "5-6 Pieces",
            "7-8 Pieces",
            "9-10 Pieces",
            ">10 Pieces",
        ]

        dsi_range = [
            "1-7 Days",
            "8-14 Days",
            "15-30 Days",
            "31-60 Days",
            "61-90 Days",
            ">90 Days",
        ]

        results = {
            "pieces_range_totals": {},
            "dsi_range_totals": {},
            "combined_totals": {},
            "grand_totals": {"pieces_range": 0, "dsi_range": 0, "overall_total": 0},
        }

        for piece_range in pieces_range:
            total_stock_sum = (
                db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                )
                .filter(
                    ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                    ExcessStockModel.SALES_NO_OF_PIECES == piece_range,
                )
                .scalar()
            ) or 0
            results["pieces_range_totals"][piece_range] = total_stock_sum
            results["grand_totals"]["pieces_range"] += total_stock_sum

        for dsi in dsi_range:
            total_stock_sum = (
                db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                )
                .filter(
                    ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                    ExcessStockModel.DSI == dsi,
                )
                .scalar()
            ) or 0
            results["dsi_range_totals"][dsi] = total_stock_sum
            results["grand_totals"]["dsi_range"] += total_stock_sum

        combined_totals = {}
        grand_totals_combined = 0
        for piece_range in pieces_range:
            for dsi in dsi_range:
                total_stock_sum = (
                    db.session.query(func.sum(ExcessStockModel.TOTAL_STOCK))
                    .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                    )
                    .filter(
                        ExcessStockModel.ST_IT_FLAG == "SALEABLE",
                        ExcessStockModel.SALES_NO_OF_PIECES == piece_range,
                        ExcessStockModel.DSI == dsi,
                    )
                    .scalar()
                ) or 0

                key = f"{piece_range} - {dsi}"
                combined_totals[key] = total_stock_sum
                grand_totals_combined += total_stock_sum

        results["combined_totals"] = combined_totals
        results["grand_totals"]["overall_total"] = grand_totals_combined

        return jsonify(results)
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_dsi_by_saleable_qty_bucket_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


# -----------------------------------------------------
def get_overall_item_level_details_controller():
    try:
        flags = ["NEVER SOLD", "NOT SOLD", "SALEABLE"]
        combined_totals = {}

        for flag in flags:
            old_stock_totals = get_overall_stock_totals_by_flag(
                flag, ExcessStockModel.OVERALL_AGE > 30
            )
            new_stock_totals = get_overall_stock_totals_by_flag(
                flag, ExcessStockModel.OVERALL_AGE <= 30
            )

            old_stock_dict = {
                item_name: total_stock for item_name, total_stock in old_stock_totals
            }
            new_stock_dict = {
                item_name: total_stock for item_name, total_stock in new_stock_totals
            }

            all_item_names = set(old_stock_dict.keys()).union(new_stock_dict.keys())

            for item_name in all_item_names:
                old_stock_total = old_stock_dict.get(item_name, 0)
                new_stock_total = new_stock_dict.get(item_name, 0)
                total_stock = old_stock_total + new_stock_total

                old_stock_percentage = (
                    (old_stock_total / total_stock * 100) if total_stock > 0 else 0
                )
                new_stock_percentage = (
                    (new_stock_total / total_stock * 100) if total_stock > 0 else 0
                )

                if item_name not in combined_totals:
                    combined_totals[item_name] = {
                        "NEVER SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "NOT SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "SALEABLE": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                    }

                combined_totals[item_name][flag] = {
                    "old_stock_total": old_stock_total,
                    "new_stock_total": new_stock_total,
                    "old_stock_percentage": old_stock_percentage,
                    "new_stock_percentage": new_stock_percentage,
                }

        return jsonify(combined_totals)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_overall_item_level_details_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_overall_stock_totals_by_flag(it_flag, age_condition):
    return (
        db.session.query(
            ExcessStockModel.ITEM_NAME,
            func.sum(ExcessStockModel.TOTAL_STOCK).label("total_stock"),
        )
        .filter(
            ExcessStockModel.SECTION == "MOBILE"
        )
        .filter(ExcessStockModel.IT_FLAG == it_flag, age_condition)
        .group_by(ExcessStockModel.ITEM_NAME)
        .all()
    )


def get_shop_level_store_level_details_controller():
    try:
        flags = ["NEVER SOLD", "NOT SOLD", "SALEABLE"]
        combined_totals = {}

        for flag in flags:
            old_stock_totals = get_shop_level_stock_totals_by_flag(
                flag, ExcessStockModel.OVERALL_AGE > 30
            )
            new_stock_totals = get_shop_level_stock_totals_by_flag(
                flag, ExcessStockModel.OVERALL_AGE <= 30
            )

            old_stock_dict = {
                store_name: total_stock for store_name, total_stock in old_stock_totals
            }
            new_stock_dict = {
                store_name: total_stock for store_name, total_stock in new_stock_totals
            }

            all_store_names = set(old_stock_dict.keys()).union(new_stock_dict.keys())

            for store_name in all_store_names:
                old_stock_total = old_stock_dict.get(store_name, 0)
                new_stock_total = new_stock_dict.get(store_name, 0)
                total_stock = old_stock_total + new_stock_total

                old_stock_percentage = (
                    (old_stock_total / total_stock * 100) if total_stock > 0 else 0
                )
                new_stock_percentage = (
                    (new_stock_total / total_stock * 100) if total_stock > 0 else 0
                )

                if store_name not in combined_totals:
                    combined_totals[store_name] = {
                        "NEVER SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "NOT SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "SALEABLE": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                    }

                combined_totals[store_name][flag] = {
                    "old_stock_total": old_stock_total,
                    "new_stock_total": new_stock_total,
                    "old_stock_percentage": old_stock_percentage,
                    "new_stock_percentage": new_stock_percentage,
                }

        return jsonify(combined_totals)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_shop_level_store_level_details_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def get_shop_level_stock_totals_by_flag(it_flag, age_condition):
    return (
        db.session.query(
            ExcessStockModel.STORE_NAME,
            func.sum(ExcessStockModel.TOTAL_STOCK).label("total_stock"),
        )
        .filter(ExcessStockModel.IT_FLAG == it_flag, age_condition)
        .filter(
                    ExcessStockModel.SECTION == "MOBILE"
                )
        .group_by(ExcessStockModel.STORE_NAME)
        .all()
    )


# -----------------------------------------------------


def overall_stock_analysis_search_controller():
    try:
        sales_category = request.args.get("salescategory")
        stock_category = request.args.get("stockcategory")
        stock_ageing = request.args.get("stockageing")
        no_of_pieces = request.args.get("noofpieces")
        dsi = request.args.get("dsi")
        store = request.args.get("store")
        brand = request.args.get("brand")
        model = request.args.get("modelno")
        section = request.args.get("section")
        state = request.args.get('state')
        city = request.args.get('city')
        store_categrory = request.args.get('storecategory')
        franch_type = request.args.get('franchtype')
        query = db.session.query(ExcessStockModel)
        item_name = request.args.get('itemname')
        
        if sales_category and sales_category !='':
            query = query.filter(ExcessStockModel.IT_FLAG == sales_category)
        if stock_category == "NEW" and stock_category != '':
            query = query.filter(ExcessStockModel.OVERALL_AGE <= 30)
        elif stock_category == "OLD" and stock_category != '':
            query = query.filter(ExcessStockModel.OVERALL_AGE > 30)
        if stock_ageing and stock_ageing != '':
            min_age, max_age = get_age_range_bounds(stock_ageing)
            if max_age:
                query = query.filter(
                    and_(
                        ExcessStockModel.OVERALL_AGE > min_age,
                        ExcessStockModel.OVERALL_AGE <= max_age,
                    )
                )
            else:
                query = query.filter(ExcessStockModel.OVERALL_AGE > min_age)
        if no_of_pieces and no_of_pieces != '':
            query = query.filter(ExcessStockModel.IT_NO_OF_PIECES == no_of_pieces)
        if dsi and dsi != '':
            query = query.filter(ExcessStockModel.IT_DSI == dsi)
        if store and store != '':
            query = query.filter(ExcessStockModel.STORE_CODE == store)
        if brand and brand != '':
            query = query.filter(ExcessStockModel.BRAND == brand)
        if model and model != '':
            query = query.filter(ExcessStockModel.MODELNO == model)
        if item_name and item_name != '':
            query = query.filter(ExcessStockModel.ITEM_NAME == item_name)
        if state and state != '':
            query = query.filter(ExcessStockModel.STATE==state)
        if city and city != '':
            query = query.filter(ExcessStockModel.CITY==city)
        if store_categrory and store_categrory != '':
            query = query.filter(ExcessStockModel.STORE_CATEGORY==store_categrory)
        if franch_type and franch_type != '':
            query = query.filter(ExcessStockModel.FRANCH_TYPE==franch_type)

            


        resp = get_section_controller().get_json()

        db_sections=resp
        db_sections=[s.lower() for s in db_sections]
        
        if section != '' and section != None:
            if section.lower() in db_sections:
                    query = query.filter(func.lower(ExcessStockModel.SECTION) == func.lower(section))

        results = query.all()

        results_dict = [
            {
                "IT_FLAG": result.IT_FLAG,
                "TOTAL_STOCK": result.TOTAL_STOCK,
                "OVERALL_AGE": result.OVERALL_AGE,
                "IT_NO_OF_PIECES": result.IT_NO_OF_PIECES,
                "SALES_IT_NO_OF_PIECES":result.SALES_IT_NO_OF_PIECES,
                "IT_DSI": result.IT_DSI,
                "MODEL_NO": result.MODELNO,
                "ITEM_NAME":result.ITEM_NAME
            }
            for result in results
        ]

        # overall_stock_position
        never_sold_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "NEVER SOLD"
        )
        not_sold_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "NOT SOLD"
        )
        saleable_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "SALEABLE"
        )

        overall_stock_position_total = stock_position__total(
            never_sold_total, not_sold_total, saleable_total
        )

        # overall_new_and_stock_position
        never_sold_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if (result["IT_FLAG"] == "NEVER SOLD" and result["OVERALL_AGE"] <= 30)
        )
        never_sold_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if (result["IT_FLAG"] == "NEVER SOLD" and result["OVERALL_AGE"] > 30)
        )
        not_sold_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "NOT SOLD" and result["OVERALL_AGE"] <= 30
        )
        not_sold_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "NOT SOLD" and result["OVERALL_AGE"] > 30
        )
        saleable_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "SALEABLE" and result["OVERALL_AGE"] <= 30
        )
        saleable_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["IT_FLAG"] == "SALEABLE" and result["OVERALL_AGE"] > 30
        )

        overall_new_and_stock_position_total = new_and_stock_position__total(
            never_sold_old_stock_total,
            never_sold_new_stock_total,
            not_sold_old_stock_total,
            not_sold_new_stock_total,
            saleable_old_stock_total,
            saleable_new_stock_total,
        )

        # overall_stock_ageing
        overall_stock_ageing = {
            "never_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "not_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "saleable": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "overall": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
        }

        category_mapping = {
            "NEVER SOLD": "never_sold",
            "NOT SOLD": "not_sold",
            "SALEABLE": "saleable",
        }

        for result in results_dict:
            category = category_mapping.get(result["IT_FLAG"], None)
            if category:
                age = result["OVERALL_AGE"]
                stock = result["TOTAL_STOCK"]

                if age <= 30:
                    overall_stock_ageing[category]["0to30_stock"] += stock
                    overall_stock_ageing["overall"]["0to30_stock"] += stock
                elif 30 < age <= 60:
                    overall_stock_ageing[category]["30to60_stock"] += stock
                    overall_stock_ageing["overall"]["30to60_stock"] += stock
                elif 60 < age <= 90:
                    overall_stock_ageing[category]["60to90_stock"] += stock
                    overall_stock_ageing["overall"]["60to90_stock"] += stock
                elif 90 < age <= 180:
                    overall_stock_ageing[category]["90to180_stock"] += stock
                    overall_stock_ageing["overall"]["90to180_stock"] += stock
                else:
                    overall_stock_ageing[category]["above_180_stock"] += stock
                    overall_stock_ageing["overall"]["above_180_stock"] += stock

                overall_stock_ageing[category]["total_stock"] += stock
                overall_stock_ageing["overall"]["total_stock"] += stock

        for category in overall_stock_ageing:
            total_stock = overall_stock_ageing[category]["total_stock"]
            if total_stock > 0:
                overall_stock_ageing[category]["0to30_stock_percentage"] = (
                    overall_stock_ageing[category]["0to30_stock"] / total_stock
                ) * 100
                overall_stock_ageing[category]["30to60_stock_percentage"] = (
                    overall_stock_ageing[category]["30to60_stock"] / total_stock
                ) * 100
                overall_stock_ageing[category]["60to90_stock_percentage"] = (
                    overall_stock_ageing[category]["60to90_stock"] / total_stock
                ) * 100
                overall_stock_ageing[category]["90to180_stock_percentage"] = (
                    overall_stock_ageing[category]["90to180_stock"] / total_stock
                ) * 100
                overall_stock_ageing[category]["above_180_stock_percentage"] = (
                    overall_stock_ageing[category]["above_180_stock"] / total_stock
                ) * 100
                overall_stock_ageing[category]["total_stock_percentage"] = 100

        # overall_sales_category_by_stock_qty_bucket
        pieces_range = [
                "1-5 Pieces",
                "6-10 Pieces",
                "11-15 Pieces",
                "16-20 Pieces",
                "21-25 Pieces",
                ">25 Pieces",
            ]
        overall_sales_category_by_stock_qty_bucket = {
            "NEVER SOLD": {},
            "SOLD WITHIN 1 MONTH": {},  # Renamed from "SALEABLE"
            "NOT SOLD > 1 MONTH": {},  # Renamed from "NOT SOLD"
            "TOTAL": {}
        }
        grand_totals = {"NEVER SOLD": 0, "SOLD WITHIN 1 MONTH": 0, "NOT SOLD > 1 MONTH": 0, "TOTAL": 0}

        # Initialize results dictionary for each piece range
        for range_value in pieces_range:
            overall_sales_category_by_stock_qty_bucket["NEVER SOLD"][range_value] = 0
            overall_sales_category_by_stock_qty_bucket["SOLD WITHIN 1 MONTH"][range_value] = 0
            overall_sales_category_by_stock_qty_bucket["NOT SOLD > 1 MONTH"][range_value] = 0
            overall_sales_category_by_stock_qty_bucket["TOTAL"][range_value] = 0

        def find_piece_range(pieces_str):
            for range_value in pieces_range:
                if pieces_str and pieces_str.startswith(range_value):
                    return range_value
            return None

        for result in results_dict:
            category = result["IT_FLAG"]
            pieces_range_value = find_piece_range(result["IT_NO_OF_PIECES"])
            if pieces_range_value:
                if category == "SALEABLE":
                    category = "SOLD WITHIN 1 MONTH"
                elif category == "NOT SOLD":
                    category = "NOT SOLD > 1 MONTH"
                
                overall_sales_category_by_stock_qty_bucket[category][pieces_range_value] += result["TOTAL_STOCK"]
                grand_totals[category] += result["TOTAL_STOCK"]

        overall_sales_category_by_stock_qty_bucket["TOTAL"] = {
            range_value: sum(
                overall_sales_category_by_stock_qty_bucket[flag].get(range_value, 0) for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]
            ) for range_value in pieces_range
        }

        grand_totals["TOTAL"] = sum(grand_totals[flag] for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"])

        # Calculate percentages
        # percentages_by_range = {range_value: {} for range_value in pieces_range}
        # for range_value in pieces_range:
        #     range_totals = sum(
        #         overall_sales_category_by_stock_qty_bucket[flag].get(range_value, 0) for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]
        #     )
        #     for category in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]:
        #         category_total = overall_sales_category_by_stock_qty_bucket[category].get(range_value, 0)
        #         percentages_by_range[range_value][category] = (
        #             (category_total / range_totals * 100) if range_totals > 0 else 0
        #         )

        # overall_sales_category_by_stock_qty_bucket["percentages"] = percentages_by_range
        overall_sales_category_by_stock_qty_bucket["grand_totals"] = grand_totals

        
        # overall_dsi_by_saleable_qty_bucket

        dsi_range = [
            "1-7 Days",
            "8-14 Days",
            "15-30 Days",
            "31-60 Days",
            "61-90 Days",
            ">90 Days",
        ]

        overall_dsi_by_saleable_qty_bucket = {
            "pieces_range_totals": {range_value: 0 for range_value in pieces_range},
            "dsi_range_totals": {range_value: 0 for range_value in dsi_range},
            "combined_totals": {},
            "grand_totals": {"pieces_range": 0, "dsi_range": 0, "overall_total": 0},
        }

        for piece_range in pieces_range:
            total_stock_sum = sum(
                result["TOTAL_STOCK"]
                for result in results_dict
                if result["SALES_IT_NO_OF_PIECES"] == piece_range
            )
            overall_dsi_by_saleable_qty_bucket["pieces_range_totals"][
                piece_range
            ] = total_stock_sum
            overall_dsi_by_saleable_qty_bucket["grand_totals"][
                "pieces_range"
            ] += total_stock_sum

        for dsi_range_item in dsi_range:
            total_stock_sum = sum(
                result["TOTAL_STOCK"]
                for result in results_dict
                if result["IT_DSI"] == dsi_range_item
            )
            overall_dsi_by_saleable_qty_bucket["dsi_range_totals"][
                dsi_range_item
            ] = total_stock_sum
            overall_dsi_by_saleable_qty_bucket["grand_totals"][
                "dsi_range"
            ] += total_stock_sum

        combined_totals = {}
        grand_totals_combined = 0
        for piece_range in pieces_range:
            for dsi_range_item in dsi_range:
                total_stock_sum = sum(
                    result["TOTAL_STOCK"]
                    for result in results_dict
                    if result["SALES_IT_NO_OF_PIECES"] == piece_range
                    and result["IT_DSI"] == dsi_range_item
                )
                key = f"{piece_range} - {dsi_range_item}"
                combined_totals[key] = total_stock_sum
                grand_totals_combined += total_stock_sum

        overall_dsi_by_saleable_qty_bucket["combined_totals"] = combined_totals
        overall_dsi_by_saleable_qty_bucket["grand_totals"][
            "overall_total"
        ] = grand_totals_combined

        # overall_item_level_details
        flags = ["NEVER SOLD", "NOT SOLD", "SALEABLE"]
        overall_item_level_details = {}

        for flag in flags:
            flag_results = [r for r in results_dict if r["IT_FLAG"] == flag]

            old_stock_totals = [r for r in flag_results if r["OVERALL_AGE"] > 30]
            new_stock_totals = [r for r in flag_results if r["OVERALL_AGE"] <= 30]

            old_stock_dict = {}
            new_stock_dict = {}

            for r in old_stock_totals:
                model_no = r["ITEM_NAME"]
                old_stock_dict[model_no] = (
                    old_stock_dict.get(model_no, 0) + r["TOTAL_STOCK"]
                )

            for r in new_stock_totals:
                model_no = r["ITEM_NAME"]
                new_stock_dict[model_no] = (
                    new_stock_dict.get(model_no, 0) + r["TOTAL_STOCK"]
                )

            all_item_names = set(old_stock_dict.keys()).union(new_stock_dict.keys())

            for item_name in all_item_names:
                old_stock_total = old_stock_dict.get(item_name, 0)
                new_stock_total = new_stock_dict.get(item_name, 0)
                total_stock = old_stock_total + new_stock_total

                old_stock_percentage = (
                    (old_stock_total / total_stock * 100) if total_stock > 0 else 0
                )
                new_stock_percentage = (
                    (new_stock_total / total_stock * 100) if total_stock > 0 else 0
                )

                if item_name not in overall_item_level_details:
                    overall_item_level_details[item_name] = {
                        "NEVER SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "NOT SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "SALEABLE": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                    }

                overall_item_level_details[item_name][flag] = {
                    "old_stock_total": old_stock_total,
                    "new_stock_total": new_stock_total,
                    "old_stock_percentage": old_stock_percentage,
                    "new_stock_percentage": new_stock_percentage,
                }

        # return jsonify(overall_stock_position_total), 200
        # return jsonify(overall_new_and_stock_position_total), 200
        # return jsonify(overall_stock_ageing), 200
        # return jsonify(overall_sales_category_by_stock_qty_bucket), 200
        # return jsonify(overall_dsi_by_saleable_qty_bucket)
        # return jsonify(overall_item_level_details)

        return (
            jsonify(
                {
                    "overall_stock_position": overall_stock_position_total,
                    "overall_new_and_stock_position": overall_new_and_stock_position_total,
                    "overall_stock_ageing": overall_stock_ageing,
                    "overall_sales_category_by_stock_qty_bucket": overall_sales_category_by_stock_qty_bucket,
                    "overall_dsi_by_saleable_qty_bucket": overall_dsi_by_saleable_qty_bucket,
                    "overall_item_level_details": overall_item_level_details,
                }
            ),
            200,
        )
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return overall_stock_analysis_search_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)



def shop_level_stock_analysis_search_controller():
    try:
        sales_category = request.args.get("salescategory")
        stock_category = request.args.get("stockcategory")
        stock_ageing = request.args.get("stockageing")
        no_of_pieces = request.args.get("noofpieces")
        dsi = request.args.get("dsi")
        store = request.args.get("store")
        brand = request.args.get("brand")
        section=request.args.get("section")
        model = request.args.get("modelno")
        item_name = request.args.get("itemname")
        state = request.args.get('state')
        city = request.args.get('city')
        store_categrory = request.args.get('storecategory')
        franch_type = request.args.get('franchtype')
        
        query = db.session.query(ExcessStockModel)

        if sales_category and sales_category != '':
            query = query.filter(ExcessStockModel.ST_IT_FLAG == sales_category)
        if stock_category == "NEW" and stock_category != '':
            query = query.filter(ExcessStockModel.OVERALL_AGE <= 30)
        elif stock_category == "OLD" and stock_category != '':
            query = query.filter(ExcessStockModel.OVERALL_AGE > 30)
        if stock_ageing and stock_ageing != '':
            min_age, max_age = get_age_range_bounds(stock_ageing)
            print(min_age, max_age)
            if max_age:
                query = query.filter(
                    and_(
                        ExcessStockModel.OVERALL_AGE > min_age,
                        ExcessStockModel.OVERALL_AGE <= max_age,
                    )
                )
            else:
                query = query.filter(ExcessStockModel.OVERALL_AGE >= min_age)
        if no_of_pieces and no_of_pieces != '':
            query = query.filter(ExcessStockModel.NO_OF_PIECES == no_of_pieces)
        if dsi and dsi != '':
            query = query.filter(ExcessStockModel.DSI == dsi)
        if store and store != '':
            query = query.filter(ExcessStockModel.STORE_CODE == store)
        if brand and brand != '':
            query = query.filter(ExcessStockModel.BRAND == brand)
        if model and model != '':
            query = query.filter(ExcessStockModel.MODELNO == model)
        if item_name and item_name != '':
            query = query.filter(ExcessStockModel.ITEM_NAME == item_name)
        if state and state != '':
            query = query.filter(ExcessStockModel.STATE==state)
        if city and city != '':
            query = query.filter(ExcessStockModel.CITY==city)
        if store_categrory and store_categrory != '':
            query = query.filter(ExcessStockModel.STORE_CATEGORY==store_categrory)
        if franch_type and franch_type != '':
            query = query.filter(ExcessStockModel.FRANCH_TYPE==franch_type)
        resp = get_section_controller().get_json()
        print(resp)
        db_sections=resp
        db_sections=[s.lower() for s in db_sections]
        print(db_sections)
        if section != '' and section != None:
            if section.lower() in db_sections:
                    query = query.filter(func.lower(ExcessStockModel.SECTION) == func.lower(section))
        print(section)
        results = query.all()
        results = query.all()

        results_dict = [
            {
                "ST_IT_FLAG": result.ST_IT_FLAG,
                "TOTAL_STOCK": result.TOTAL_STOCK,
                "OVERALL_AGE": result.OVERALL_AGE,
                "NO_OF_PIECES": result.NO_OF_PIECES,
                "SALES_NO_OF_PIECES":result.SALES_NO_OF_PIECES,
                "DSI": result.DSI,
                "MODEL_NO": result.MODELNO,
                "STORE_NAME": result.STORE_NAME,
            }
            for result in results
        ]

        # shop_level_stock_position
        never_sold_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "NEVER SOLD"
        )
        not_sold_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "NOT SOLD"
        )
        saleable_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "SALEABLE"
        )

        shop_level_stock_position_total = stock_position__total(
            never_sold_total, not_sold_total, saleable_total
        )

        # shop_level_new_and_stock_position
        never_sold_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if (result["ST_IT_FLAG"] == "NEVER SOLD" and result["OVERALL_AGE"] <= 30)
        )
        never_sold_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if (result["ST_IT_FLAG"] == "NEVER SOLD" and result["OVERALL_AGE"] > 30)
        )
        not_sold_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "NOT SOLD" and result["OVERALL_AGE"] <= 30
        )
        not_sold_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "NOT SOLD" and result["OVERALL_AGE"] > 30
        )
        saleable_new_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "SALEABLE" and result["OVERALL_AGE"] <= 30
        )
        saleable_old_stock_total = sum(
            result["TOTAL_STOCK"]
            for result in results_dict
            if result["ST_IT_FLAG"] == "SALEABLE" and result["OVERALL_AGE"] > 30
        )

        shop_level_new_and_stock_position_total = new_and_stock_position__total(
            never_sold_old_stock_total,
            never_sold_new_stock_total,
            not_sold_old_stock_total,
            not_sold_new_stock_total,
            saleable_old_stock_total,
            saleable_new_stock_total,
        )

        # shop_level_stock_ageing
        shop_level_stock_ageing = {
            "never_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "not_sold": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "saleable": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
            "overall": {
                "0to30_stock": 0,
                "30to60_stock": 0,
                "60to90_stock": 0,
                "90to180_stock": 0,
                "above_180_stock": 0,
                "0to30_stock_percentage": 0,
                "30to60_stock_percentage": 0,
                "60to90_stock_percentage": 0,
                "90to180_stock_percentage": 0,
                "above_180_stock_percentage": 0,
                "total_stock": 0,
                "total_stock_percentage": 0,
            },
        }

        category_mapping = {
            "NEVER SOLD": "never_sold",
            "NOT SOLD": "not_sold",
            "SALEABLE": "saleable",
        }

        for result in results_dict:
            category = category_mapping.get(result["ST_IT_FLAG"], None)
            if category:
                age = result["OVERALL_AGE"]
                stock = result["TOTAL_STOCK"]

                if age <= 30:
                    shop_level_stock_ageing[category]["0to30_stock"] += stock
                    shop_level_stock_ageing["overall"]["0to30_stock"] += stock
                elif 30 < age <= 60:
                    shop_level_stock_ageing[category]["30to60_stock"] += stock
                    shop_level_stock_ageing["overall"]["30to60_stock"] += stock
                elif 60 < age <= 90:
                    shop_level_stock_ageing[category]["60to90_stock"] += stock
                    shop_level_stock_ageing["overall"]["60to90_stock"] += stock
                elif 90 < age <= 180:
                    shop_level_stock_ageing[category]["90to180_stock"] += stock
                    shop_level_stock_ageing["overall"]["90to180_stock"] += stock
                else:
                    shop_level_stock_ageing[category]["above_180_stock"] += stock
                    shop_level_stock_ageing["overall"]["above_180_stock"] += stock

                shop_level_stock_ageing[category]["total_stock"] += stock
                shop_level_stock_ageing["overall"]["total_stock"] += stock

        for category in shop_level_stock_ageing:
            total_stock = shop_level_stock_ageing[category]["total_stock"]
            if total_stock > 0:
                shop_level_stock_ageing[category]["0to30_stock_percentage"] = (
                    shop_level_stock_ageing[category]["0to30_stock"] / total_stock
                ) * 100
                shop_level_stock_ageing[category]["30to60_stock_percentage"] = (
                    shop_level_stock_ageing[category]["30to60_stock"] / total_stock
                ) * 100
                shop_level_stock_ageing[category]["60to90_stock_percentage"] = (
                    shop_level_stock_ageing[category]["60to90_stock"] / total_stock
                ) * 100
                shop_level_stock_ageing[category]["90to180_stock_percentage"] = (
                    shop_level_stock_ageing[category]["90to180_stock"] / total_stock
                ) * 100
                shop_level_stock_ageing[category]["above_180_stock_percentage"] = (
                    shop_level_stock_ageing[category]["above_180_stock"] / total_stock
                ) * 100
                shop_level_stock_ageing[category]["total_stock_percentage"] = 100

        # shop_level_sales_category_by_stock_qty_bucket

        pieces_range = [
                "1-2 Pieces",
                "3-4 Pieces",
                "5-6 Pieces",
                "7-8 Pieces",
                "9-10 Pieces",
                ">10 Pieces",
            ]
        shop_level_sales_category_by_stock_qty_bucket = {
            "NEVER SOLD": {},
            "SOLD WITHIN 1 MONTH": {},  # Renamed from "SALEABLE"
            "NOT SOLD > 1 MONTH": {},  # Renamed from "NOT SOLD"
            "TOTAL": {}
        }
        grand_totals = {"NEVER SOLD": 0, "SOLD WITHIN 1 MONTH": 0, "NOT SOLD > 1 MONTH": 0, "TOTAL": 0}

        # Initialize the shop_level_sales_category_by_stock_qty_bucket
        for range_value in pieces_range:
            shop_level_sales_category_by_stock_qty_bucket["NEVER SOLD"][range_value] = 0
            shop_level_sales_category_by_stock_qty_bucket["SOLD WITHIN 1 MONTH"][range_value] = 0
            shop_level_sales_category_by_stock_qty_bucket["NOT SOLD > 1 MONTH"][range_value] = 0
            shop_level_sales_category_by_stock_qty_bucket["TOTAL"][range_value] = 0

        def find_piece_range(pieces_str):
            if pieces_str:
                for range_value in pieces_range:
                    if pieces_str.startswith(range_value):
                        return range_value
            return None

        for result in results_dict:
            category = result["ST_IT_FLAG"]
            pieces_range_value = find_piece_range(result["NO_OF_PIECES"])
            if pieces_range_value:
                if category == "SALEABLE":
                    category = "SOLD WITHIN 1 MONTH"
                elif category == "NOT SOLD":
                    category = "NOT SOLD > 1 MONTH"
                
                shop_level_sales_category_by_stock_qty_bucket[category][pieces_range_value] += result["TOTAL_STOCK"]
                grand_totals[category] += result["TOTAL_STOCK"]

        # Calculate TOTAL values
        shop_level_sales_category_by_stock_qty_bucket["TOTAL"] = {
            range_value: sum(
                shop_level_sales_category_by_stock_qty_bucket[flag].get(range_value, 0) for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]
            ) for range_value in pieces_range
        }

        grand_totals["TOTAL"] = sum(grand_totals[flag] for flag in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"])

        # percentages_by_range = {range_value: {} for range_value in pieces_range}
        # for range_value in pieces_range:
        #     range_totals = sum(
        #         shop_level_sales_category_by_stock_qty_bucket[label].get(range_value, 0)
        #         for label in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]
        #     )
        #     for label in ["NEVER SOLD", "SOLD WITHIN 1 MONTH", "NOT SOLD > 1 MONTH"]:
        #         category_total = shop_level_sales_category_by_stock_qty_bucket[label].get(range_value, 0)
        #         percentages_by_range[range_value][label] = (
        #             (category_total / range_totals * 100) if range_totals > 0 else 0
        #         )

        # shop_level_sales_category_by_stock_qty_bucket["percentages"] = percentages_by_range
        shop_level_sales_category_by_stock_qty_bucket["grand_totals"] = grand_totals





        # shop_level_dsi_by_saleable_qty_bucket
        dsi_range = [
            "1-7 Days",
            "8-14 Days",
            "15-30 Days",
            "31-60 Days",
            "61-90 Days",
            ">90 Days",
        ]

        shop_level_dsi_by_saleable_qty_bucket = {
            "pieces_range_totals": {range_value: 0 for range_value in pieces_range},
            "dsi_range_totals": {range_value: 0 for range_value in dsi_range},
            "combined_totals": {},
            "grand_totals": {"pieces_range": 0, "dsi_range": 0, "overall_total": 0},
        }

        for piece_range in pieces_range:
            total_stock_sum = sum(
                result["TOTAL_STOCK"]
                for result in results_dict
                if result["SALES_NO_OF_PIECES"] == piece_range
            )
            shop_level_dsi_by_saleable_qty_bucket["pieces_range_totals"][
                piece_range
            ] = total_stock_sum
            shop_level_dsi_by_saleable_qty_bucket["grand_totals"][
                "pieces_range"
            ] += total_stock_sum

        for dsi_range_item in dsi_range:
            total_stock_sum = sum(
                result["TOTAL_STOCK"]
                for result in results_dict
                if result["DSI"] == dsi_range_item
            )
            shop_level_dsi_by_saleable_qty_bucket["dsi_range_totals"][
                dsi_range_item
            ] = total_stock_sum
            shop_level_dsi_by_saleable_qty_bucket["grand_totals"][
                "dsi_range"
            ] += total_stock_sum

        combined_totals = {}
        grand_totals_combined = 0
        for piece_range in pieces_range:
            for dsi_range_item in dsi_range:
                total_stock_sum = sum(
                    result["TOTAL_STOCK"]
                    for result in results_dict
                    if result["SALES_NO_OF_PIECES"] == piece_range
                    and result["DSI"] == dsi_range_item
                )
                key = f"{piece_range} - {dsi_range_item}"
                combined_totals[key] = total_stock_sum
                grand_totals_combined += total_stock_sum

        shop_level_dsi_by_saleable_qty_bucket["combined_totals"] = combined_totals
        shop_level_dsi_by_saleable_qty_bucket["grand_totals"][
            "overall_total"
        ] = grand_totals_combined

        # shop_level_item_level_details
        flags = ["NEVER SOLD", "NOT SOLD", "SALEABLE"]
        shop_level_item_level_details = {}

        for flag in flags:
            flag_results = [r for r in results_dict if r["ST_IT_FLAG"] == flag]

            old_stock_totals = [r for r in flag_results if r["OVERALL_AGE"] > 30]
            new_stock_totals = [r for r in flag_results if r["OVERALL_AGE"] <= 30]

            old_stock_dict = {}
            new_stock_dict = {}

            for r in old_stock_totals:
                store_name = r["STORE_NAME"]
                old_stock_dict[store_name] = (
                    old_stock_dict.get(store_name, 0) + r["TOTAL_STOCK"]
                )

            for r in new_stock_totals:
                store_name = r["STORE_NAME"]
                new_stock_dict[store_name] = (
                    new_stock_dict.get(store_name, 0) + r["TOTAL_STOCK"]
                )

            all_store_names = set(old_stock_dict.keys()).union(new_stock_dict.keys())

            for store_name in all_store_names:
                old_stock_total = old_stock_dict.get(store_name, 0)
                new_stock_total = new_stock_dict.get(store_name, 0)
                total_stock = old_stock_total + new_stock_total

                old_stock_percentage = (
                    (old_stock_total / total_stock * 100) if total_stock > 0 else 0
                )
                new_stock_percentage = (
                    (new_stock_total / total_stock * 100) if total_stock > 0 else 0
                )

                if store_name not in shop_level_item_level_details:
                    shop_level_item_level_details[store_name] = {
                        "NEVER SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "NOT SOLD": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                        "SALEABLE": {
                            "old_stock_total": 0,
                            "new_stock_total": 0,
                            "old_stock_percentage": 0,
                            "new_stock_percentage": 0,
                        },
                    }

                shop_level_item_level_details[store_name][flag] = {
                    "old_stock_total": old_stock_total,
                    "new_stock_total": new_stock_total,
                    "old_stock_percentage": old_stock_percentage,
                    "new_stock_percentage": new_stock_percentage,
                }

        # return jsonify(shop_level_stock_position_total), 200
        # return jsonify(shop_level_new_and_stock_position_total), 200
        # return jsonify(shop_level_stock_ageing), 200
        # return jsonify(shop_level_sales_category_by_stock_qty_bucket), 200
        # return jsonify(shop_level_dsi_by_saleable_qty_bucket)
        # return jsonify(shop_level_item_level_details)

        return (
            jsonify(
                {
                    "overall_stock_position": shop_level_stock_position_total,
                    "shop_level_new_and_stock_position": shop_level_new_and_stock_position_total,
                    "shop_level_stock_ageing": shop_level_stock_ageing,
                    "shop_level_sales_category_by_stock_qty_bucket": shop_level_sales_category_by_stock_qty_bucket,
                    "shop_level_dsi_by_saleable_qty_bucket": shop_level_dsi_by_saleable_qty_bucket,
                    "shop_level_item_level_details": shop_level_item_level_details,
                }
            ),
            200,
        )
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return shop_level_stock_analysis_search_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)



# Utils

def stock_position__total(never_sold_total, not_sold_total, saleable_total):
    try:
        total = never_sold_total + not_sold_total + saleable_total

        if total > 0:
            never_sold_percentage = (never_sold_total / total) * 100
            not_sold_percentage = (not_sold_total / total) * 100
            saleable_percentage = (saleable_total / total) * 100

            total_percentage = (
                never_sold_percentage + not_sold_percentage + saleable_percentage
            )
            normalization_factor = 100 / total_percentage

            never_sold_percentage *= normalization_factor
            not_sold_percentage *= normalization_factor
            saleable_percentage *= normalization_factor
        else:
            never_sold_percentage = not_sold_percentage = saleable_percentage = 0

        overall_stock_position = {
            "never_sold_total": never_sold_total,
            "not_sold_total": not_sold_total,
            "saleable_total": saleable_total,
            "total": total,
            "never_sold_percentage": never_sold_percentage,
            "not_sold_percentage": not_sold_percentage,
            "saleable_percentage": saleable_percentage,
            "total_percentage": 100,
        }

        return overall_stock_position
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return stock_position__total(never_sold_total,not_sold_total,saleable_total)
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)



def new_and_stock_position__total(
    never_sold_old_stock_total,
    never_sold_new_stock_total,
    not_sold_old_stock_total,
    not_sold_new_stock_total,
    saleable_old_stock_total,
    saleable_new_stock_total,
):
   
    total_stock = (
        never_sold_old_stock_total
        + never_sold_new_stock_total
        + not_sold_old_stock_total
        + not_sold_new_stock_total
        + saleable_old_stock_total
        + saleable_new_stock_total
    )

    never_sold_old_stock_percent = (
        (never_sold_old_stock_total / total_stock) * 100 if total_stock else 0
    )
    never_sold_new_stock_percent = (
        (never_sold_new_stock_total / total_stock) * 100 if total_stock else 0
    )
    not_sold_old_stock_percent = (
        (not_sold_old_stock_total / total_stock) * 100 if total_stock else 0
    )
    not_sold_new_stock_percent = (
        (not_sold_new_stock_total / total_stock) * 100 if total_stock else 0
    )
    saleable_old_stock_percent = (
        (saleable_old_stock_total / total_stock) * 100 if total_stock else 0
    )
    saleable_new_stock_percent = (
        (saleable_new_stock_total / total_stock) * 100 if total_stock else 0
    )

    overall_new_and_stock_position = {
        "never_sold_old_stock_total": never_sold_old_stock_total,
        "never_sold_new_stock_total": never_sold_new_stock_total,
        "not_sold_old_stock_total": not_sold_old_stock_total,
        "not_sold_new_stock_total": not_sold_new_stock_total,
        "saleable_old_stock_total": saleable_old_stock_total,
        "saleable_new_stock_total": saleable_new_stock_total,
        "total_stock": total_stock,
        "percentages": {
            "never_sold_old_stock_percent": never_sold_old_stock_percent,
            "never_sold_new_stock_percent": never_sold_new_stock_percent,
            "not_sold_old_stock_percent": not_sold_old_stock_percent,
            "not_sold_new_stock_percent": not_sold_new_stock_percent,
            "saleable_old_stock_percent": saleable_old_stock_percent,
            "saleable_new_stock_percent": saleable_new_stock_percent,
        },
    }

    return overall_new_and_stock_position
