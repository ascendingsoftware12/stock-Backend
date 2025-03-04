from flask import request, jsonify
from sqlalchemy import case, func,or_,text
from src import db
from collections import defaultdict
from src.models.stock_all_in_one_summary_model import StockAllInOneSummary
from datetime import datetime
import traceback
import re

def search_StockAnalysis_kore_common_controller():
    try:
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        branch_code = request.args.get('storecode')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))

        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.total_sales / StockAllInOneSummary.sales_qty
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
        
        if overall_age is not None:
            if overall_age == "BLANK":
                conditions.append(StockAllInOneSummary.overall_age.is_(None))
            elif 0 <= overall_age <= 7:
                conditions.append((StockAllInOneSummary.overall_age >= 0) & (StockAllInOneSummary.overall_age <= 7))
            elif 8 <= overall_age <= 14:
                conditions.append((StockAllInOneSummary.overall_age >= 8) & (StockAllInOneSummary.overall_age <= 14))
            elif 15 <= overall_age <= 21:
                conditions.append((StockAllInOneSummary.overall_age >= 15) & (StockAllInOneSummary.overall_age <= 21))
            elif 22 <= overall_age <= 28:
                conditions.append((StockAllInOneSummary.overall_age >= 22) & (StockAllInOneSummary.overall_age <= 28))
            elif 29 <= overall_age <= 90:
                conditions.append((StockAllInOneSummary.overall_age >= 29) & (StockAllInOneSummary.overall_age <= 90))
            elif 91 <= overall_age <= 180:
                conditions.append((StockAllInOneSummary.overall_age >= 91) & (StockAllInOneSummary.overall_age <= 180))
            elif 181 <= overall_age <= 270:
                conditions.append((StockAllInOneSummary.overall_age >= 181) & (StockAllInOneSummary.overall_age <= 270))
            elif 271 <= overall_age <= 365:
                conditions.append((StockAllInOneSummary.overall_age >= 271) & (StockAllInOneSummary.overall_age <= 365))
            elif overall_age >= 366:
                conditions.append(StockAllInOneSummary.overall_age >= 366)

        if purchase_rate:
            try:
                purchase_rate = float(purchase_rate)
                if 0 <= purchase_rate <= 3000:
                    conditions.append((StockAllInOneSummary.purchase_rate >= 0) & (StockAllInOneSummary.purchase_rate <= 3000))
                elif 3000 < purchase_rate <= 5000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 3000) & (StockAllInOneSummary.purchase_rate <= 5000))
                elif 5000 < purchase_rate <= 8000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 5000) & (StockAllInOneSummary.purchase_rate <= 8000))
                elif 8000 < purchase_rate <= 10000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 8000) & (StockAllInOneSummary.purchase_rate <= 10000))
                elif 10000 < purchase_rate <= 15000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 10000) & (StockAllInOneSummary.purchase_rate <= 15000))
                elif 15000 < purchase_rate <= 20000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 15000) & (StockAllInOneSummary.purchase_rate <= 20000))
                elif 20000 < purchase_rate <= 30000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 20000) & (StockAllInOneSummary.purchase_rate <= 30000))
                elif 30000 < purchase_rate <= 40000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 30000) & (StockAllInOneSummary.purchase_rate <= 40000))
                elif 40000 < purchase_rate <= 70000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 40000) & (StockAllInOneSummary.purchase_rate <= 70000))
                elif 70000 < purchase_rate <= 100000:
                    conditions.append((StockAllInOneSummary.purchase_rate > 70000) & (StockAllInOneSummary.purchase_rate <= 100000))
                elif purchase_rate > 100000:
                    conditions.append(StockAllInOneSummary.purchase_rate > 100000)
            except ValueError:
                # Handle invalid input gracefully
                return jsonify({"success": 0, "error": "Invalid selling price input"})

        return conditions
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_common_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def get_StockAnlaysis_kore_all_in_column_live_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        branch_code = request.args.get('branch_code')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        if branch_code and branch_code != '':
            conditions.append(StockAllInOneSummary.branch_code == branch_code)

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

        # Define the columns to fetch with aggregation
        query = db.session.query(
            StockAllInOneSummary.section,
            StockAllInOneSummary.brand,
            StockAllInOneSummary.modelno,
            StockAllInOneSummary.demo_flag,
            StockAllInOneSummary.item_name
        ).filter(*conditions)

        # Use distinct to get unique values
        distinct_data = query.distinct().all()

        # Process the result set into a dictionary
        sales_data = {
            "section": set(),
            "brand": set(),
            "modelno": set(),
            "item_name": set(),
            "demo_flag": set(),
        }

        for record in distinct_data:
            sales_data["section"].add(record.section)
            sales_data["brand"].add(record.brand)
            sales_data["modelno"].add(record.modelno)
            sales_data["item_name"].add(record.item_name)
            sales_data["demo_flag"].add(record.demo_flag)

        # Convert sets to lists for JSON serialization
        sales_data = {key: list(value) for key, value in sales_data.items()}

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            # Retry logic for database connection error
            return get_StockAnlaysis_kore_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

    except Exception as e:
        return jsonify({"success": 0, "error": str(e)})

def search_StockAnalysis_kore_quantity_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        branch_code = request.args.get('storecode')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))

        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

            
        qty_query = db.session.query(
            func.sum(StockAllInOneSummary.qty).label('Quantity')
        ).filter(*conditions)
        Quantity = qty_query.scalar()

        if Quantity is None:
            Quantity = 0  

       
        qty_result_data = {
            "Quantity": Quantity
        }

        return jsonify({
            "success": 1,
            "data": qty_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_StockAnalysis_kore_quantity_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})

def search_StockAnalysis_kore_Selling_Price_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        branch_code = request.args.get('storecode')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
       
        purchase_rate_query = db.session.query(
            func.sum(StockAllInOneSummary.purchase_rate).label('Purchase_rate')
        ).filter(*conditions)

        Purchase_rate = purchase_rate_query.scalar()

        if Purchase_rate is None:
            Purchase_rate = 0 

        formatted_purchase_rate = int(Purchase_rate)

        purchase_rate_result_data = {
            "Purchase_rate": formatted_purchase_rate
        }

        return jsonify({
            "success": 1,
            "data": purchase_rate_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_StockAnalysis_kore_Selling_Price_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})

def search_StockAnalysis_kore_No_Of_Brands_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        no_of_brands_query = db.session.query(
            func.count(func.distinct(StockAllInOneSummary.brand)).label('No_Of_Brands')
        ).filter(*conditions)

        No_Of_Brands = no_of_brands_query.scalar()

        if No_Of_Brands is None:
            No_Of_Brands = 0  

        no_of_brands_result_data = {
            "No_Of_Brands": No_Of_Brands
        }

        return jsonify({
            "success": 1,
            "data": no_of_brands_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_StockAnalysis_kore_No_Of_Brands_controller() 
        else:
            return jsonify({"success": 0, "error": error_message})

def search_StockAnalysis_kore_No_Of_Items_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        no_of_items_query = db.session.query(
            func.count(func.distinct(StockAllInOneSummary.item_code)).label('No_Of_Items')
        ).filter(*conditions)

       
        No_Of_Items = no_of_items_query.scalar()

        if No_Of_Items is None:
            No_Of_Items = 0  

        
        no_of_items_result_data = {
            "No_Of_Items": No_Of_Items
        }

        return jsonify({
            "success": 1,
            "data": no_of_items_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_StockAnalysis_kore_No_Of_Items_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})

def search_StockAnalysis_kore_Ageing_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        overall_age_query = db.session.query(
            func.avg(StockAllInOneSummary.overall_age).label('overall_age')
        ).filter(*conditions)

        
        overall_age = overall_age_query.scalar()

        if overall_age is None:
            overall_age = 0  

        ageing_result_data = {
            "total_average": overall_age  
        }

        return jsonify({
            "success": 1,
            "data": ageing_result_data
        })

    except Exception as e:
        
        db.session.rollback()

        error_message = str(e)

        
        if "MySQL server has gone away" in error_message:
            
            return search_StockAnalysis_kore_Ageing_controller()
        else:
            
            return jsonify({"success": 0, "error": error_message})

def search_StockAnalysis_kore_Holding_Cost_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

        # Start building the holding value query
        holding_value_query = db.session.query(
            func.sum(
                (StockAllInOneSummary.purchase_rate * StockAllInOneSummary.qty) / 365 * 0.09 * StockAllInOneSummary.overall_age
            ).label('holding_value')
        ).filter(StockAllInOneSummary.overall_age > 30)

        # Apply additional conditions from search_StockAnalysis_common_controller
        for condition in conditions:
            holding_value_query = holding_value_query.filter(condition)

        # Execute the query
        holding_value = holding_value_query.scalar()

        if holding_value is None:
            holding_value = 0  

        holding_value_result_data = {
            "holding_value": int(holding_value)
        }

        return jsonify({
            "success": 1,
            "data": holding_value_result_data
        })

    except Exception as e:
        db.session.rollback()

        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_StockAnalysis_kore_Holding_Cost_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})


def search_StockAnalysis_kore_section_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        section_query = db.session.query(
            StockAllInOneSummary.section.label('section'),
            func.sum(StockAllInOneSummary.purchase_rate).label('value'),
            func.sum(StockAllInOneSummary.qty).label('qty'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')            
        ).filter(*conditions).group_by(StockAllInOneSummary.section).order_by(
            func.sum(StockAllInOneSummary.purchase_rate).desc()  # Order by value in descending order
        )

        
        section_result  = section_query.limit(limit).offset(offset).all()
        section_result_data = []

        for row in section_result:

            section_result_data.append({
                "section": row.section,
                "value": int(row.value),
                "qty": int(row.qty),                
                "value_sum_percentage":f"{row.Percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": section_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_section_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def search_StockAnalysis_kore_brand_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        brand_query = db.session.query(
            StockAllInOneSummary.brand.label('brand'),
            func.sum(StockAllInOneSummary.purchase_rate).label('value'),
            func.sum(StockAllInOneSummary.qty).label('qty'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')            
        ).filter(*conditions).group_by(StockAllInOneSummary.brand).order_by(
            func.sum(StockAllInOneSummary.purchase_rate).desc()  # Order by value in descending order
        )

        # item_category_query = db.session.query(item_category_query).limit(limit).offset(offset)

        # item_category_result = item_category_query.all()

        brand_result = brand_query.limit(limit).offset(offset).all()

        brand_result_data = []

        for row in brand_result:

            brand_result_data.append({
                "brand": row.brand,
                "value": int(row.value),
                "qty": int(row.qty),                
                "value_sum_percentage":f"{row.Percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": brand_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_brand_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_StockAnalysis_kore_ModelNo_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age is not None:
            age_map = {
                "BLANK": None,
                "0-7": (0, 7),
                "8-14": (8, 14),
                "15-21": (15, 21),
                "22-28": (22, 28),
                "29-90": (29, 90),
                "91-180": (91, 180),
                "181-270": (181, 270),
                "271-365": (271, 365),
                "366+": (366, None)
            }
            age_range = age_map.get(overall_age)
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))
            
        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        modelno_query = db.session.query(
            StockAllInOneSummary.modelno.label('modelno'),
            func.sum(StockAllInOneSummary.purchase_rate).label('value'),
            func.sum(StockAllInOneSummary.qty).label('qty'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')            
        ).filter(*conditions).group_by(StockAllInOneSummary.modelno).order_by(
            func.sum(StockAllInOneSummary.purchase_rate).desc()  # Order by value in descending order
        )

        # item_category_query = db.session.query(item_category_query).limit(limit).offset(offset)

        # item_category_result = item_category_query.all()

        modelno_result = modelno_query.limit(limit).offset(offset).all()

        modelno_result_data = []

        for row in modelno_result:
            
            modelno_result_data.append({
                "modelno": row.modelno,
                "value": int(row.value),
                "qty": int(row.qty),                
                "value_sum_percentage":f"{row.Percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": modelno_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_ModelNo_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_StockAnalysis_kore_Item_controller():

    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')
        branch_code = request.args.get('storecode')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        # if item_name and item_name != '':
        #     item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
        #     conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        item_name_query = db.session.query(
            StockAllInOneSummary.item_name.label('item_name'),
            func.sum(StockAllInOneSummary.purchase_rate).label('value'),
            func.sum(StockAllInOneSummary.qty).label('qty'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')            
        ).filter(*conditions).group_by(StockAllInOneSummary.item_name).order_by(
            func.sum(StockAllInOneSummary.purchase_rate).desc()  # Order by value in descending order
        )

        # item_category_query = db.session.query(item_category_query).limit(limit).offset(offset)

        # item_category_result = item_category_query.all()

        item_name_result = item_name_query.limit(limit).offset(offset).all()

        item_name_result_data = []

        for row in item_name_result:
            
            item_name_result_data.append({
                "item_name": row.item_name,
                "value": int(row.value),
                "qty": int(row.qty),                
                "value_sum_percentage":f"{row.Percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": item_name_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_Item_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# def search_StockAnalysis_Overall_Bucket_controller():

#     try:
#         # Query data grouped by sale_type and including sales_qty
#         overall_age_query = db.session.query(
#             StockAllInOneSummary.overall_age.label('overall'),
#             func.sum(StockAllInOneSummary.selling_price).label('value'),
#             func.sum(StockAllInOneSummary.qty).label('qty'),
#         ).group_by(StockAllInOneSummary.overall_age)

#         # Fetch results
#         overall_age_result = overall_age_query.all()

#         # Age bucket conditions
#         age_buckets = {
#             "upto 1 week": {},
#             "1-2 weeks": {},
#             "2-3 weeks": {},
#             "3-4 weeks": {},
#             "1-3 Months": {},
#             "4-6 Months": {},
#             "7-9 Months": {},
#             "9-12 Months": {},
#             "More than a year": {},
#         }

#         # Calculate total sales percentage and ASP
#         value_sum = sum(row.value for row in overall_age_result)
#         overall_age_result_data = []

#         for row in overall_age_result:
#             # Determine the age bucket name
#             if row.overall is None:
#                 age_bucket = "Null"
#             elif 0 <= row.overall <= 7:
#                 age_bucket = "upto 1 week"
#             elif 8 <= row.overall <= 14:
#                 age_bucket = "1-2 weeks"
#             elif 15 <= row.overall <= 21:
#                 age_bucket = "2-3 weeks"
#             elif 22 <= row.overall <= 28:
#                 age_bucket = "3-4 weeks"
#             elif 29 <= row.overall <= 90:
#                 age_bucket = "1-3 Months"
#             elif 91 <= row.overall <= 180:
#                 age_bucket = "4-6 Months"
#             elif 181 <= row.overall <= 270:
#                 age_bucket = "7-9 Months"
#             elif 271 <= row.overall <= 365:
#                 age_bucket = "9-12 Months"
#             elif row.overall >= 366:
#                 age_bucket = "More than a year"

#             # Calculate sales percentage
#             value_sum_percentage = (row.value / value_sum) * 100 if value_sum else 0
#             value_sum_percentage_str = f"{value_sum_percentage:.2f}%"

#             # Append the calculated data to the response list
#             overall_age_result_data.append({
#                 "overall_age": row.overall,
#                 "age_bucket": age_bucket,
#                 "value": int(row.value),
#                 "qty": int(row.qty),
#                 "value_sum_percentage": value_sum_percentage_str
#             })

#         return jsonify({
#             "success": 1,
#             "data": overall_age_result_data
#         })

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return search_StockAnalysis_Overall_Bucket_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})

def search_StockAnalysis_kore_Overall_Bucket_controller():

    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        branch_code = request.args.get('storecode')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))

        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))
        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        query = db.session.query(
            case(
                (StockAllInOneSummary.overall_age.is_(None), 'Null'),
                (StockAllInOneSummary.overall_age.between(0, 7), 'upto 1 week'),
                (StockAllInOneSummary.overall_age.between(8, 14), '1-2 weeks'),
                (StockAllInOneSummary.overall_age.between(15, 21), '2-3 weeks'),
                (StockAllInOneSummary.overall_age.between(22, 28), '3-4 weeks'),
                (StockAllInOneSummary.overall_age.between(29, 90), '1-3 Months'),
                (StockAllInOneSummary.overall_age.between(91, 180), '4-6 Months'),
                (StockAllInOneSummary.overall_age.between(181, 270), '7-9 Months'),
                (StockAllInOneSummary.overall_age.between(271, 365), '9-12 Months'),
                (StockAllInOneSummary.overall_age >= 366, 'More than a year'),
                else_='Unknown'
            ).label('Overall_Age_Bucket'),
            func.sum(StockAllInOneSummary.purchase_rate).label('Total_Selling_Price'),
            func.sum(StockAllInOneSummary.qty).label('Total_qty'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')
        ).filter(*conditions).group_by('Overall_Age_Bucket').offset(offset).limit(limit)
        
        result = query.all()

        overall_age_result_data = [
            {
                "overall_age_bucket": row.Overall_Age_Bucket,
                "total_selling_price": int(row.Total_Selling_Price),
                "Total_qty":int(row.Total_qty),
                "percentage": f"{row.Percentage:.2f}%"
            }
            for row in result
        ]

        return jsonify({
            "success": 1,
            "data": overall_age_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_Overall_Bucket_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_StockAnalysis_kore_Price_Bucket_controller():
    try:
        
        section = request.args.get('section')
        # sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product = request.args.get('product')
        brand = request.args.get('brand')
        modelno = request.args.get('modelno')
        # branch_name = request.args.get('branch_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        overall_age = request.args.get('overall_age')
        selling_price = request.args.get('selling_price')
        # asm = request.args.get('asm')
        item_name = request.args.get('item_name')
        branch_code = request.args.get('storecode')
        # IMEI_STATUS = request.args.get('IMEI_STATUS')

        conditions = []
        price_conditions = []

        # if IMEI_STATUS and IMEI_STATUS != '':
        #     if IMEI_STATUS == 'NON IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status == 'NON IMEI')
        #     elif IMEI_STATUS == 'IMEI':
        #         conditions.append(StockAllInOneSummary.imei_status != 'NON IMEI')

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(StockAllInOneSummary.section.in_(section_list))

        # if product and product != '':
        #     product_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product) if isinstance(product, str) else product
        #     conditions.append(StockAllInOneSummary.product.in_(product_list))
        
        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(StockAllInOneSummary.item_category.in_(item_category_list))

        if brand and brand != '':
            brand_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand) if isinstance(brand, str) else brand
            conditions.append(StockAllInOneSummary.brand.in_(brand_list))

        if modelno and modelno != '':
            modelno_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', modelno) if isinstance(modelno, str) else modelno
            conditions.append(StockAllInOneSummary.modelno.in_(modelno_list))

        if item_name and item_name != '':
            item_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_name) if isinstance(item_name, str) else item_name
            conditions.append(StockAllInOneSummary.item_name.in_(item_name_list))
        if branch_code and branch_code != '':
            branch_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_code) if isinstance(branch_code, str) else branch_code
            conditions.append(StockAllInOneSummary.branch_code.in_(branch_code_list))
        # if asm and asm != '':
        #     conditions.append(StockAllInOneSummary.asm == asm)

        # if branch_name and branch_name != '':
        #     branch_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', branch_name) if isinstance(branch_name, str) else branch_name
        #     conditions.append(StockAllInOneSummary.branch_name.in_(branch_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(StockAllInOneSummary.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag_list))

        if price_breakup2:
            sales_per_unit = StockAllInOneSummary.purchase_rate
            price_breakup2_list = price_breakup2.split(',') if isinstance(price_breakup2, str) else price_breakup2
            
            # Create a list to hold the sub-conditions for price ranges            
            for price_range in price_breakup2_list:
                if price_range == '0-3000':
                    price_conditions.append(sales_per_unit <= 3000)
                elif price_range == '3001-5000':
                    price_conditions.append(sales_per_unit.between(3000, 5000))
                elif price_range == '5001-8000':
                    price_conditions.append((sales_per_unit > 5000) & (sales_per_unit <= 8000))
                elif price_range == '8001-10000':
                    price_conditions.append((sales_per_unit > 8000) & (sales_per_unit <= 10000))
                elif price_range == '10001-15000':
                    price_conditions.append((sales_per_unit > 10000) & (sales_per_unit <= 15000))
                elif price_range == '15001-20000':
                    price_conditions.append((sales_per_unit > 15000) & (sales_per_unit <= 20000))
                elif price_range == '20001-30000':
                    price_conditions.append((sales_per_unit > 20000) & (sales_per_unit <= 30000))
                elif price_range == '30001-40000':
                    price_conditions.append((sales_per_unit > 30000) & (sales_per_unit <= 40000))
                elif price_range == '40001-70000':
                    price_conditions.append((sales_per_unit > 40000) & (sales_per_unit <= 70000))
                elif price_range == '70001-100000':
                    price_conditions.append((sales_per_unit > 70000) & (sales_per_unit <= 100000))
                elif price_range == '>100000':
                    price_conditions.append(sales_per_unit > 100000)
            # Combine all the sub-conditions using OR logic
        if price_conditions:
         conditions.append(or_(*price_conditions))

        if overall_age and overall_age != '':
            if overall_age is not None:
                age_map = {
                    "BLANK": None,
                    "0-7": (0, 7),
                    "8-14": (8, 14),
                    "15-21": (15, 21),
                    "22-28": (22, 28),
                    "29-90": (29, 90),
                    "91-180": (91, 180),
                    "181-270": (181, 270),
                    "271-365": (271, 365),
                    "366+": (366, None)
                }
                age_range = age_map.get(overall_age)
                # print("y")
                # print(age_range)
                if age_range is None:
                    conditions.append(StockAllInOneSummary.overall_age.is_(None))
                else:
                    if age_range[1] is None:
                        # print(age_range[0])
                        conditions.append(StockAllInOneSummary.overall_age >= age_range[0])
                    else:
                        # print(age_range)
                        conditions.append(StockAllInOneSummary.overall_age.between(*age_range))

        # query = db.session.query(
        #     case(
        #         (StockAllInOneSummary.selling_price <= 3000, '0-3000'),
        #         (StockAllInOneSummary.selling_price.between(3001, 5000), '3001-5000'),
        #         (StockAllInOneSummary.selling_price.between(5001, 8000), '5001-8000'),
        #         (StockAllInOneSummary.selling_price.between(8001, 10000), '8001-10000'),
        #         (StockAllInOneSummary.selling_price.between(10001, 15000), '10001-15000'),
        #         (StockAllInOneSummary.selling_price.between(15001, 20000), '15001-20000'),
        #         (StockAllInOneSummary.selling_price.between(20001, 30000), '20001-30000'),
        #         (StockAllInOneSummary.selling_price.between(30001, 40000), '30001-40000'),
        #         (StockAllInOneSummary.selling_price.between(40001, 70000), '40001-70000'),
        #         (StockAllInOneSummary.selling_price.between(70001, 100000), '70001-100000'),
        #         (StockAllInOneSummary.selling_price > 100000, '>100000'),
        #     ).label('Price_Bucket'),
        #     func.sum(StockAllInOneSummary.selling_price).label('Total_Selling_Price'),
        #     func.sum(StockAllInOneSummary.qty).label('Total_Quantity'),
        #     (func.sum(StockAllInOneSummary.selling_price) / 
        #      func.coalesce(
        #          db.session.query(func.sum(StockAllInOneSummary.selling_price)).scalar_subquery(), 
        #          1
        #      ) * 100).label('Percentage')
        # ).filter(*conditions).group_by('Price_Bucket')


        total_purchase_rate_subquery = (
            db.session.query(func.sum(StockAllInOneSummary.purchase_rate))
            .filter(*conditions)
            .scalar_subquery()
        )

        # Main query with SQLAlchemy
        query = (
            db.session.query(
                case(
                    (StockAllInOneSummary.purchase_rate <= 3000, '0-3000'),
                    (StockAllInOneSummary.purchase_rate.between(3001, 5000), '3001-5000'),
                    (StockAllInOneSummary.purchase_rate.between(5001, 8000), '5001-8000'),
                    (StockAllInOneSummary.purchase_rate.between(8001, 10000), '8001-10000'),
                    (StockAllInOneSummary.purchase_rate.between(10001, 15000), '10001-15000'),
                    (StockAllInOneSummary.purchase_rate.between(15001, 20000), '15001-20000'),
                    (StockAllInOneSummary.purchase_rate.between(20001, 30000), '20001-30000'),
                    (StockAllInOneSummary.purchase_rate.between(30001, 40000), '30001-40000'),
                    (StockAllInOneSummary.purchase_rate.between(40001, 70000), '40001-70000'),
                    (StockAllInOneSummary.purchase_rate.between(70001, 100000), '70001-100000'),
                    (StockAllInOneSummary.purchase_rate > 100000, '>100000'),
                    else_=None
                ).label('Price_Bucket'),
                func.sum(StockAllInOneSummary.purchase_rate).label('Total_Selling_Price'),
                func.sum(StockAllInOneSummary.qty).label('Total_Quantity'),
                func.round(
                    (func.sum(StockAllInOneSummary.purchase_rate) / total_purchase_rate_subquery) * 100, 2
                ).label('Percentage')
            )
            .filter(*conditions)
            .group_by('Price_Bucket')
        )

        result = query.all()

        # Format the result
        response_data = [
            {
                "price_bucket": row.Price_Bucket,
                "total_selling_price": float(row.Total_Selling_Price),
                "total_quantity": int(row.Total_Quantity),
                "percentage": f"{row.Percentage:.2f}%"
            }
            for row in result
        ]

        return jsonify({"success": 1, "data": response_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_StockAnalysis_kore_Price_Bucket_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# def get_StockAnlaysis_all_in_column_live_controller():
#     try:
#         section = request.args.get('section')
#         item_category = request.args.get('item_category')
#         product = request.args.get('product')
#         brand = request.args.get('brand')
#         modelno = request.args.get('modelno')
#         branch_name = request.args.get('branch_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         overall_age = request.args.get('overall_age')
#         selling_price = request.args.get('selling_price')
#         asm = request.args.get('asm')
#         item_name = request.args.get('item_name')
#         IMEI_STATUS = request.args.get('IMEI_STATUS')

#         conditions = []
#         price_conditions = []

#         # Handle filters
#         if asm:
#             conditions.append(StockAllInOneSummary.asm == asm)
#         if IMEI_STATUS:
#             conditions.append(StockAllInOneSummary.IMEI_STATUS.in_(IMEI_STATUS.split(',')))
#         if product:
#             conditions.append(StockAllInOneSummary.product.in_(product.split(',')))
#         if modelno:
#             conditions.append(StockAllInOneSummary.modelno.in_(modelno.split(',')))
#         if branch_name:
#             conditions.append(StockAllInOneSummary.branch_name.in_(branch_name.split(',')))
#         if section:
#             conditions.append(StockAllInOneSummary.section.in_(section.split(',')))
#         if item_name:
#             conditions.append(StockAllInOneSummary.item_name.in_(item_name.split(',')))
#         if item_category:
#             conditions.append(StockAllInOneSummary.item_category.in_(item_category.split(',')))
#         if brand:
#             conditions.append(StockAllInOneSummary.brand.in_(brand.split(',')))
#         if city:
#             conditions.append(StockAllInOneSummary.city.in_(city.split(',')))
#         if demo_flag:
#             conditions.append(StockAllInOneSummary.demo_flag.in_(demo_flag.split(',')))

#         # Price breakup condition
#         if price_breakup2:
#             sales_per_unit = StockAllInOneSummary.total_sales / StockAllInOneSummary.sales_qty
#         # Overall age condition
#         age_map = {
#             "0-7": (0, 7), "8-14": (8, 14), "15-21": (15, 21),
#             "22-28": (22, 28), "29-90": (29, 90), "91-180": (91, 180),
#             "181-270": (181, 270), "271-365": (271, 365), "366+": (366, None)
#         }
#         if overall_age in age_map:
#             min_age, max_age = age_map[overall_age]
#             if max_age:
#                 conditions.append(StockAllInOneSummary.overall_age.between(min_age, max_age))
#             else:
#                 conditions.append(StockAllInOneSummary.overall_age >= min_age)

#         # Selling price condition
#         if selling_price:
#             try:
#                 selling_price = float(selling_price)
#                 conditions.append(StockAllInOneSummary.selling_price <= selling_price)
#             except ValueError:
#                 return jsonify({"success": 0, "error": "Invalid selling price input"}), 400

#         # Query with conditions
#         query = db.session.query(
#             StockAllInOneSummary.branch_name,
#             StockAllInOneSummary.city,
#             StockAllInOneSummary.section,
#             StockAllInOneSummary.item_category,
#             StockAllInOneSummary.product,
#             StockAllInOneSummary.brand,
#             StockAllInOneSummary.modelno,
#             StockAllInOneSummary.item_name,
#             StockAllInOneSummary.demo_flag
#         ).filter(*conditions).distinct()

#         # Process the result set
#         distinct_data = query.all()
#         sales_data = {
#             "branch_name": set(),
#             "city": set(),
#             "section": set(),
#             "item_category": set(),
#             "product": set(),
#             "brand": set(),
#             "modelno": set(),
#             "item_name": set(),
#             "demo_flag": set(),
#         }

#         for record in distinct_data:
#             sales_data["branch_name"].add(record.branch_name)
#             sales_data["city"].add(record.city)
#             sales_data["section"].add(record.section)
#             sales_data["item_category"].add(record.item_category)
#             sales_data["product"].add(record.product)
#             sales_data["brand"].add(record.brand)
#             sales_data["modelno"].add(record.modelno)
#             sales_data["item_name"].add(record.item_name)
#             sales_data["demo_flag"].add(record.demo_flag)

#         # Convert sets to lists for JSON serialization
#         sales_data = {key: list(value) for key, value in sales_data.items()}

#         return jsonify(sales_data)

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"success": 0, "error": str(e)})

def get_stock_table_kore_modification_date_and_time_controller():
    try:
        # Query to get the latest update time from DATA_REFRESH_TIME column
        query = text("""
            SELECT MAX(DATA_REFRESH_TIME) AS Latest_Update 
            FROM apx_stock_apps.stock
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

