from flask import request, jsonify
from sqlalchemy import Float, case, cast, func,or_,and_
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
from datetime import datetime
import traceback
import re

# ---------------------------------------------------

def search_PeriodComparison_common_controller():
    try:
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # srn_flag = request.args.get('srn_flag')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')
        conditions = []

        # if srn_flag and srn_flag != '':
        #     conditions.append(SalesAllInOneLive.srn_flag == srn_flag)

        if sales_type and sales_type != '':
            sale_type_list = sales_type.split(',') if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = section.split(',') if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = product_group.split(',') if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = item_category.split(',') if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = brand_name.split(',') if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = model_no.split(',') if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = item_description.split(',') if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = store_name.split(',') if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = city.split(',') if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = demo_flag.split(',') if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # if gstfillter and gstfillter != '' and gstfillter == 'withGSTCr':
        #     conditions.append(SalesAllInOneLive.total_sales/10000000)

        # if gstfillter and gstfillter != '' and gstfillter=='withoutGSTCR':
        #     conditions.append((SalesAllInOneLive.total_sales -SalesAllInOneLive.tax_amt) / 10000000)

        # if gstfillter and gstfillter != '' and gstfillter=='withGSTLk':
        #     conditions.append(SalesAllInOneLive.total_sales/100000)

        # if gstfillter and gstfillter != '' and gstfillter=='withoutGSTLk':
        #     conditions.append((SalesAllInOneLive.total_sales - SalesAllInOneLive.tax_amt )/100000)

        # if gstfillter and gstfillter != '' and gstfillter=='salesqty':
        #     conditions.append(SalesAllInOneLive.sales_qty)

        # if gstfillter and gstfillter != '' and gstfillter=='totalsales':
        #     conditions.append(SalesAllInOneLive.total_sales)
        
        # if gstfillter and gstfillter != '' and gstfillter=='gpLk':
        #     conditions.append(SalesAllInOneLive. gros_profit/100000)

        if price_breakup2 and price_breakup2 != '':
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
        

        return conditions
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_common_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})
# --------------------SALES--------------------
def search_PeriodComparison_sales_kore_controller():
    try:
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []
        
        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), SalesAllInOneLive.total_sales),
                else_=0
            )
        ).label('period1_total_sales')

        period2_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), SalesAllInOneLive.total_sales),
                else_=0
            )
        ).label('period2_total_sales')        

        query = db.session.query(
            period1_total_sales,
            period2_total_sales
        ).filter(*conditions)

        result = query.all()
        result_data = []
        for row in result:
            period1_sales = row.period1_total_sales or 0
            period2_sales = row.period2_total_sales or 0

            if period1_sales != 0:
                growth_percentage = ((period2_sales - period1_sales) / period1_sales) * 100
            else:
                growth_percentage = None  

            result_data.append({
                "period1_total_sales": period1_sales,
                "period2_total_sales": period2_sales,
                "growth_percentage": growth_percentage
            })
        return jsonify({"success": 1, "data": result_data})
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_sales_kore_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------DISCOUNT AMOUNT--------------------------
def search_PeriodComparison_kore_DisAmt_controller():
    try:
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})
        
        period1_dis_amt = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), SalesAllInOneLive.disc_amt),
                else_=0
            )
        ).label('period1_dis_amt')

        period2_dis_amt = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), SalesAllInOneLive.disc_amt),
                else_=0
            )
        ).label('period2_dis_amt')        

        query = db.session.query(
            period1_dis_amt,
            period2_dis_amt
        ).filter(*conditions)

        result = query.all()
        result_data = []
        for row in result:
            period1_disc_amt = row.period1_dis_amt or 0  # Default to 0 if None
            period2_disc_amt = row.period2_dis_amt or 0  # Default to 0 if None

            if period2_disc_amt != 0:
                growth_percentage = ((period1_disc_amt - period2_disc_amt) / period2_disc_amt) * 100
            else:
                growth_percentage = 0  

            result_data.append({
                "period1_total_sales": period1_disc_amt,
                "period2_total_sales": period2_disc_amt,
                "growth_percentage": growth_percentage
            })
        return jsonify({"success": 1, "data": result_data})
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_DisAmt_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------SALES QUENTITY--------------------------
def search_PeriodComparison_kore_SalesQty_controller():
    try:
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_sales_qty = func.coalesce(
            func.sum(
                case(
                    (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), SalesAllInOneLive.sales_qty),
                    else_=0
                )
            ), 0
        ).label('period1_sales_qty')

        period2_sales_qty = func.coalesce(
            func.sum(
                case(
                    (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), SalesAllInOneLive.sales_qty),
                    else_=0
                )
            ), 0
        ).label('period2_sales_qty')

        query = db.session.query(
            period1_sales_qty,
            period2_sales_qty
        ).filter(*conditions)

        result = query.all()
        if not result:
            return jsonify({"success": 1, "data": []})

        result_data = []
        for row in result:
            period1_Sales_qty = row.period1_sales_qty or 0
            period2_Sales_qty = row.period2_sales_qty or 0

            if period1_Sales_qty != 0:
                growth_percentage = ((period2_Sales_qty - period1_Sales_qty) / period1_Sales_qty) * 100
            else:
                growth_percentage = None  

            result_data.append({
                "period1_total_sales": period1_Sales_qty,
                "period2_total_sales": period2_Sales_qty,
                "growth_percentage": growth_percentage
            })
        return jsonify({"success": 1, "data": result_data})
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_SalesQty_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------DISCOUNT%--------------------------
def search_PeriodComparison_kore_dis_controller():
    try:
        # Extract parameters from request
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), SalesAllInOneLive.total_sales),
                else_=0
            )
        ).label('period1_total_sales')

        period2_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), SalesAllInOneLive.total_sales),
                else_=0
            )
        ).label('period2_total_sales')

        period1_discount_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), SalesAllInOneLive.disc_amt),
                else_=0
            )
        ).label('period1_discount_sales')

        period2_discount_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), SalesAllInOneLive.disc_amt),
                else_=0
            )
        ).label('period2_discount_sales')

        query = db.session.query(
            period1_total_sales,
            period2_total_sales,
            period1_discount_sales,
            period2_discount_sales
        ).filter(*conditions)

        result = query.all()
        result_data = []
        for row in result:
            period1_sales = row.period1_total_sales or 0.0
            period2_sales = row.period2_total_sales or 0.0
            period1_discount = row.period1_discount_sales or 0.0
            period2_discount = row.period2_discount_sales or 0.0
            period1sales = period1_discount + period1_sales
            period2sales = period2_discount + period2_sales
            period1_discount_ratio = round((period1_discount / period1sales) * 100,2) if period1_sales != 0 else 0.0
            period2_discount_ratio = round((period2_discount / period2sales) * 100,2)  if period2_sales != 0 else 0.0

            if period2_discount_ratio != 0:
                growth_percentage = ((period1_discount_ratio - period2_discount_ratio) / period2_discount_ratio) * 100
            else:
                growth_percentage = 0.0

            result_data.append({
                "growth_percentage": growth_percentage,
                "period1_discount_ratio": period1_discount_ratio,
                "period2_discount_ratio": period2_discount_ratio
            })

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_dis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


# -------------------ASP--------------------------
def search_PeriodComparison_kore_asp_controller():
    try:
        # Extract query parameters
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        # Validate input
        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        # Define calculated fields
        period1_total_sales = func.sum(
                    case(
                        (
                            SalesAllInOneLive.invoice_date.between(period1_from, period1_to),
                            SalesAllInOneLive.total_sales
                        ),
                        else_=0
                    )
                ).label("period1_total_sales")

        period2_total_sales = func.sum(
                    case(
                        (
                            SalesAllInOneLive.invoice_date.between(period2_from, period2_to),
                            SalesAllInOneLive.total_sales
                        ),
                        else_=0
                    )
                ).label("period2_total_sales")

        period1_sales_qty = func.sum(
                    case(
                        (
                            SalesAllInOneLive.invoice_date.between(period1_from, period1_to),
                            SalesAllInOneLive.sales_qty
                        ),
                        else_=0
                    )
                ).label("period1_sales_qty")

        period2_sales_qty = func.sum(
                    case(
                        (
                            SalesAllInOneLive.invoice_date.between(period2_from, period2_to),
                            SalesAllInOneLive.sales_qty
                        ),
                        else_=0
                    )
                ).label("period2_sales_qty")

                # Construct query
        query = db.session.query(
                    period1_total_sales,
                    period2_total_sales,
                    period1_sales_qty,
                    period2_sales_qty
                ).filter(*conditions)

                # Execute query
        result = query.all()

                # Process results
        result_data = []
        for row in result:
                    period1_total_sales_value = row.period1_total_sales or 0
                    period2_total_sales_value = row.period2_total_sales or 0
                    period1_sales_qty_value = row.period1_sales_qty or 0
                    period2_sales_qty_value = row.period2_sales_qty or 0

                    # Calculate average sales for each period
                    period1_avg_sales = period1_total_sales_value / period1_sales_qty_value if period1_sales_qty_value else 0
                    period2_avg_sales = period2_total_sales_value / period2_sales_qty_value if period2_sales_qty_value else 0

                    # Calculate growth percentage
                    if period1_total_sales_value != 0:
                        growth_percentage = ((period2_avg_sales - period1_avg_sales) / period1_avg_sales) * 100
                    else:
                        growth_percentage = None

                    # Append result
                    result_data.append({
                        "period1_avg_sales": period1_avg_sales,
                        "period2_avg_sales": period2_avg_sales,
                        "growth_percentage": growth_percentage
                    })

                # Return response
        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        # Rollback transaction on error
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_asp_controller()  # Retry logic
        else:
            return jsonify({"success": 0, "error": str(e)})


# -------------------SECTION WISE ANALYSIS--------------------------
def search_PeriodComparison_kore_section_wise_Analysis_controller():
    try:
        # Fetch additional filters from request arguments
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit   
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        gstfillter = request.args.get('gstfilter', 'totalsales')
        

        if gstfillter == 'totalsales':
            total_sales_field = SalesAllInOneLive.total_sales
        elif gstfillter == 'salesqty':
            total_sales_field = SalesAllInOneLive.sales_qty
        elif gstfillter == 'dis':
            total_sales_field = SalesAllInOneLive.disc_amt
        else:
            total_sales_field = SalesAllInOneLive.total_sales 

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_section_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), total_sales_field),
                else_=0
            )
        ).label('period1_section_total_sales')

        period2_section_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), total_sales_field),
                else_=0
            )
        ).label('period2_section_total_sales')

        if gstfillter == 'dis':
            growth_percentage = (
            (period1_section_total_sales - period2_section_total_sales) /
            period2_section_total_sales * 100
        ).label('growth_percentage')
        else:
            growth_percentage = (
            (period2_section_total_sales - period1_section_total_sales) /
            period1_section_total_sales * 100
        ).label('growth_percentage')
        

        query = db.session.query(
            SalesAllInOneLive.section.label('section'),
            period1_section_total_sales,
            period2_section_total_sales,
            growth_percentage
        ).filter(*conditions
        ).group_by(SalesAllInOneLive.section
        ).having(
            (period1_section_total_sales != 0) | (period2_section_total_sales != 0)
        ).order_by(period2_section_total_sales.desc())
        result = query.limit(limit).offset(offset).all()
        result_data = []
        for row in result:
            result_data.append({
                "section": row.section,
                "period1_section_total_sales": row.period1_section_total_sales,
                "period2_section_total_sales": row.period2_section_total_sales,
                "growth_percentage": row.growth_percentage
            })

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_section_wise_Analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------BRAND WISE ANALYSIS--------------------------
def search_PeriodComparison_kore_brand_wise_Analysis_controller():

    try:
        # Fetch additional filters from request arguments
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))
                    # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit            
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        gstfillter = request.args.get('gstfilter', 'totalsales')

        if gstfillter == 'totalsales':
            total_sales_field = SalesAllInOneLive.total_sales
        elif gstfillter == 'salesqty':
            total_sales_field = SalesAllInOneLive.sales_qty
        elif gstfillter == 'dis':
            total_sales_field = SalesAllInOneLive.disc_amt
        else:
            total_sales_field = SalesAllInOneLive.total_sales 

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        
        period1_brand_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), total_sales_field),
                else_=0
            )
        ).label('period1_brand_total_sales')

        period2_brand_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), total_sales_field),
                else_=0
            )
        ).label('period2_brand_total_sales')


        if gstfillter == 'dis':
            growth_percentage = (
            (period1_brand_total_sales - period2_brand_total_sales) /
            period2_brand_total_sales * 100
        ).label('growth_percentage')
        else:
            growth_percentage = (
            (period2_brand_total_sales - period1_brand_total_sales) /
            period1_brand_total_sales * 100
        ).label('growth_percentage')
        

        query = db.session.query(
            SalesAllInOneLive.brand_name.label('brand_name'),
            period1_brand_total_sales,
            period2_brand_total_sales,
            growth_percentage
        ).filter(*conditions).group_by(SalesAllInOneLive.brand_name
        ).order_by(period2_brand_total_sales.desc())
        result = query.limit(limit).offset(offset).all()
        result_data = []
        for row in result:
            result_data.append({
                "brand_name": row.brand_name,
                "period1_brand_total_sales": row.period1_brand_total_sales,
                "period2_brand_total_sales": row.period2_brand_total_sales,
                "growth_percentage": row.growth_percentage
            })

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_brand_wise_Analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------ITEM WISE ANALYSIS--------------------------
def search_PeriodComparison_kore_item_wise_Analysis_controller():
    try:
        print("ggggg")
        # Fetch additional filters from request arguments
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')
        srn_flag = request.args.get('srn_flag')

        # Build conditions list
        conditions = []
        if srn_flag:
            srn_flag_list =  re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)

        if product_group and product_group != '':
            product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        if item_category and item_category != '':
            item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        if store_name and store_name != '':
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        if city and city != '':
            city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")

        gstfillter = request.args.get('gstfilter', 'totalsales')
        
        if gstfillter == 'totalsales':
            total_sales_field = SalesAllInOneLive.total_sales
        elif gstfillter == 'salesqty':
            total_sales_field = SalesAllInOneLive.sales_qty
        elif gstfillter == 'dis':
            total_sales_field = SalesAllInOneLive.disc_amt
        else:
            total_sales_field = SalesAllInOneLive.total_sales 

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_item_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), total_sales_field),
                else_=0
            )
        ).label('period1_item_total_sales')

        period2_item_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), total_sales_field),
                else_=0
            )
        ).label('period2_item_total_sales')

        if gstfillter == 'dis':
            growth_percentage = (
            (period1_item_total_sales - period2_item_total_sales) /
            period2_item_total_sales * 100
        ).label('growth_percentage')
        else:
            growth_percentage = (
            (period2_item_total_sales - period1_item_total_sales) /
            period2_item_total_sales * 100
        ).label('growth_percentage')
        

        query = db.session.query(
            SalesAllInOneLive.item_description.label('item_description'),
            period1_item_total_sales,
            period2_item_total_sales,
            growth_percentage
        ).filter(*conditions).group_by(SalesAllInOneLive.item_description
        ).order_by(period2_item_total_sales.desc())
        result = query.limit(limit).offset(offset)
        result_data = []
        for row in result:
            result_data.append({
                "item_description": row.item_description,
                "period1_item_total_sales": row.period1_item_total_sales,
                "period2_item_total_sales": row.period2_item_total_sales,
                "growth_percentage": row.growth_percentage
            })

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_item_wise_Analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# -------------------PRICE WISE ANALYSIS--------------------------
def search_PeriodComparison_kore_price_wise_Analysis_controller():
    try:
        # Retrieve query parameters
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")
        # Fetch additional filters from request arguments
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        # item_category = request.args.get('item_category')
        # product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        # if product_group and product_group != '':
        #     product_group_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        # if item_category and item_category != '':
        #     item_category_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        # if store_name and store_name != '':
        #     store_name_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        # if city and city != '':
        #     city_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]\([^\)]\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        gstfillter = request.args.get('gstfilter', 'totalsales')
       

        if gstfillter == 'totalsales':
            total_sales_field = SalesAllInOneLive.total_sales
        elif gstfillter == 'salesqty':
            total_sales_field = SalesAllInOneLive.sales_qty
        elif gstfillter == 'dis':
            total_sales_field = SalesAllInOneLive.disc_amt
        else:
            total_sales_field = SalesAllInOneLive.total_sales 

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        # Define period totals
        period1_price_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), total_sales_field),
                else_=0
            )
        ).label('period1_price_total_sales')

        period2_price_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), total_sales_field),
                else_=0
            )
        ).label('period2_price_total_sales')

        # Growth percentage calculation
        growth_percentage = cast(
            (period2_price_total_sales - period1_price_total_sales) / 
            (period1_price_total_sales) * 100, Float
        ).label('growth_percentage')

        # Price breakup calculation
        price_breakup = case(
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 0,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 5000), '0-5000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 5000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 10000), '5001-10000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 10000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 15000), '10001-15000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 15000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 20000), '15001-20000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 20000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 25000), '20001-25000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 25000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 50000), '25001-50000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 50000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 70000), '50001-70000'),
            (and_(SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 70000,
                SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) <= 100000), '70001-100000'),
            (SalesAllInOneLive.total_sales / func.nullif(SalesAllInOneLive.sales_qty, 0) > 100000, '>100000'),
            else_='Null'
        )

        # Determine the fiscal year
        fiscal_year = case(
            (func.month(SalesAllInOneLive.invoice_date) >= 4, func.year(SalesAllInOneLive.invoice_date) + 1),
            else_=func.year(SalesAllInOneLive.invoice_date)
        )

        # Query the database
        result = db.session.query(
            fiscal_year.label('fiscal_year'),
            price_breakup.label('price_breakup'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('total_qty'),
            func.sum(SalesAllInOneLive.tax_amt).label('tax_amt'),
            func.sum(SalesAllInOneLive.gros_profit).label('gros_profit'),
            period1_price_total_sales,
            period2_price_total_sales,
            growth_percentage
        ).filter(*conditions).group_by(
            fiscal_year, price_breakup
        ).order_by(
            fiscal_year.desc()
        ).all()

        # Initialize output dictionaries
        output = {"values": {}, "years": sorted({row.fiscal_year for row in result}, reverse=True)}
        price_ranges = {}


        # Process results and assign to price ranges
        for row in result:
            price_breakup = row.price_breakup
            period1_sales = row.period1_price_total_sales or 0
            period2_sales = row.period2_price_total_sales or 0
            growth = row.growth_percentage or 0
            if price_breakup not in price_ranges:
                price_ranges[price_breakup] = {
                    "period1_price_total_sales": 0,
                    "period2_price_total_sales": 0,
                    "growth_percentage": 0
                }
            if price_ranges[price_breakup]["period1_price_total_sales"] != 0:
                    if gstfillter == 'dis':
                        # Growth percentage calculation
                        growth_percentage1 = (price_ranges[price_breakup]["period1_price_total_sales"] - price_ranges[price_breakup]["period2_price_total_sales"]) /price_ranges[price_breakup]["period2_price_total_sales"] * 100
                    else:
                        # Growth percentage calculation
                        growth_percentage1 = (price_ranges[price_breakup]["period2_price_total_sales"] - price_ranges[price_breakup]["period1_price_total_sales"]) /price_ranges[price_breakup]["period1_price_total_sales"] * 100
                    
                    price_ranges[price_breakup]["growth_percentage"] = growth_percentage1
            
            price_ranges[price_breakup]["period1_price_total_sales"] += period1_sales
            price_ranges[price_breakup]["period2_price_total_sales"] += period2_sales
            

        # Final output
        output["values"] = price_ranges


        return jsonify({"success": 1, "values": price_ranges})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_kore_price_wise_Analysis_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})
def search_PeriodComparison_item_wise_Analysis_controller():
    try:
        # Fetch additional filters from request arguments
        section = request.args.get('section')
        sales_type = request.args.get('sales_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        item_description = request.args.get('item_description')        
        store_code = request.args.get('storecode')

        # Build conditions list
        conditions = []

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

        # if asm and asm != '':
        #     conditions.append(SalesAllInOneLive.asm == asm)

        if product_group and product_group != '':
            product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
            conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

        if item_category and item_category != '':
            item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
            conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

        if brand_name and brand_name != '':
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

        if model_no and model_no != '':
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

        if item_description and item_description != '':
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

        if store_name and store_name != '':
            store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
            conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

        if city and city != '':
            city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
            conditions.append(SalesAllInOneLive.city.in_(city_list))

        if demo_flag and demo_flag != '':
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
            conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

        # Handle price_breakup2 filtering
        if price_breakup2:
            avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_ranges = {
                '0-5000': avg_price.between(0, 5000),
                '5001-10000': (avg_price > 5000) & (avg_price <= 10000),
                '10001-15000': (avg_price > 10000) & (avg_price <= 15000),
                '15001-20000': (avg_price > 15000) & (avg_price <= 20000),
                '20001-25000': (avg_price > 20000) & (avg_price <= 25000),
                '25001-50000': (avg_price > 25000) & (avg_price <= 50000),
                '50001-70000': (avg_price > 50000) & (avg_price <= 70000),
                '70001-100000': (avg_price > 70000) & (avg_price <= 100000),
                '>100000': avg_price > 100000,
            }
            if price_breakup2 in price_ranges:
                conditions.append(price_ranges[price_breakup2])

        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        period1_from = request.args.get("period1_from")
        period1_to = request.args.get("period1_to")
        period2_from = request.args.get("period2_from")
        period2_to = request.args.get("period2_to")

        gstfillter = request.args.get('gstfilter', 'totalsales')
        
        if gstfillter == 'totalsales':
            total_sales_field = SalesAllInOneLive.total_sales
        elif gstfillter == 'salesqty':
            total_sales_field = SalesAllInOneLive.sales_qty
        elif gstfillter == 'dis':
            total_sales_field = SalesAllInOneLive.disc_amt
        else:
            total_sales_field = SalesAllInOneLive.total_sales 

        if not all([period1_from, period1_to, period2_from, period2_to]):
            return jsonify({"success": 0, "error": "All period parameters are required."})

        period1_item_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period1_from, period1_to), total_sales_field),
                else_=0
            )
        ).label('period1_item_total_sales')

        period2_item_total_sales = func.sum(
            case(
                (SalesAllInOneLive.invoice_date.between(period2_from, period2_to), total_sales_field),
                else_=0
            )
        ).label('period2_item_total_sales')

        if gstfillter == 'dis':
            growth_percentage = (
            (period1_item_total_sales - period2_item_total_sales) /
            period2_item_total_sales * 100
        ).label('growth_percentage')
        else:
            growth_percentage = (
            (period2_item_total_sales - period1_item_total_sales) /
            period1_item_total_sales * 100
        ).label('growth_percentage')
        

        query = db.session.query(
            SalesAllInOneLive.item_description.label('item_description'),
            period1_item_total_sales,
            period2_item_total_sales,
            growth_percentage
        ).filter(*conditions).group_by(SalesAllInOneLive.item_description
        ).order_by(period2_item_total_sales.desc())
        result = query.limit(limit).offset(offset)
        result_data = []
        for row in result:
            result_data.append({
                "item_description": row.item_description,
                "period1_item_total_sales": row.period1_item_total_sales,
                "period2_item_total_sales": row.period2_item_total_sales,
                "growth_percentage": row.growth_percentage
            })

        return jsonify({"success": 1, "data": result_data})

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_PeriodComparison_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

# def get_PeriodComparison_all_in_column_live_controller():
#     try:
#         # Fetch additional filters from request arguments
#         section = request.args.get('section')
#         sales_type = request.args.get('sales_type')
#         item_category = request.args.get('item_category')
#         product_group = request.args.get('product_group')
#         brand_name = request.args.get('brand_name')
#         model_no = request.args.get('model_no')
#         store_name = request.args.get('store_name')
#         city = request.args.get('city')
#         demo_flag = request.args.get('demo_flag')
#         price_breakup2 = request.args.get('PriceBreakup2')
#         asm = request.args.get('asm')
#         item_description = request.args.get('item_description')        
#         store_code = request.args.get('storecode')

#         # Build conditions list
#         conditions = []

#         if store_code and store_code != '':
#             store_code_list = store_code.split(',') if isinstance(store_code, str) else store_code
#             conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

#         if sales_type and sales_type != '':
#             sale_type_list = sales_type.split(',') if isinstance(sales_type, str) else sales_type
#             conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

#         if section and section != '':
#             section_list = section.split(',') if isinstance(section, str) else section
#             conditions.append(SalesAllInOneLive.section.in_(section_list))

#         if asm and asm != '':
#             conditions.append(SalesAllInOneLive.asm == asm)

#         if product_group and product_group != '':
#             product_group_list = product_group.split(',') if isinstance(product_group, str) else product_group
#             conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))

#         if item_category and item_category != '':
#             item_category_list = item_category.split(',') if isinstance(item_category, str) else item_category
#             conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))

#         if brand_name and brand_name != '':
#             brand_name_list = brand_name.split(',') if isinstance(brand_name, str) else brand_name
#             conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))

#         if model_no and model_no != '':
#             model_no_list = model_no.split(',') if isinstance(model_no, str) else model_no
#             conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))

#         if item_description and item_description != '':
#             item_description_list = item_description.split(',') if isinstance(item_description, str) else item_description
#             conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))

#         if store_name and store_name != '':
#             store_name_list = store_name.split(',') if isinstance(store_name, str) else store_name
#             conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))

#         if city and city != '':
#             city_list = city.split(',') if isinstance(city, str) else city
#             conditions.append(SalesAllInOneLive.city.in_(city_list))

#         if demo_flag and demo_flag != '':
#             demo_flag_list = demo_flag.split(',') if isinstance(demo_flag, str) else demo_flag
#             conditions.append(SalesAllInOneLive.demo_flag.in_(demo_flag_list))

#         # Handle price_breakup2 filtering
#         if price_breakup2:
#             avg_price = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
#             price_ranges = {
#                 '0-1000': avg_price <= 1000,
#                 '1001-2000': avg_price.between(1000, 2000),
#                 '2001-3000': (avg_price > 2000) & (avg_price <= 3000),
#                 '3001-4000': (avg_price > 3000) & (avg_price <= 4000),
#                 '4001-5000': (avg_price > 4000) & (avg_price <= 5000),
#                 '5001-6000': (avg_price > 5000) & (avg_price <= 6000),
#                 '6001-7000': (avg_price > 6000) & (avg_price <= 7000),
#                 '7001-8000': (avg_price > 7000) & (avg_price <= 8000),
#                 '8001-9000': (avg_price > 8000) & (avg_price <= 9000),
#                 '9001-10000': (avg_price > 9000) & (avg_price <= 10000),
#                 '10001-20000': (avg_price > 10000) & (avg_price <= 20000),
#                 '20001-30000': (avg_price > 20000) & (avg_price <= 30000),
#                 '30001-40000': (avg_price > 30000) & (avg_price <= 40000),
#                 '40001-50000': (avg_price > 40000) & (avg_price <= 50000),
#                 '>50000': avg_price > 50000,
#             }
#             if price_breakup2 in price_ranges:
#                 conditions.append(price_ranges[price_breakup2])

#         period1_from = request.args.get("period1_from")
#         period1_to = request.args.get("period1_to")
#         period2_from = request.args.get("period2_from")
#         period2_to = request.args.get("period2_to")
        
#         if not isinstance(conditions, list):
#             conditions = []

#         def parse_date(date_str):
#             return datetime.strptime(date_str, "%Y-%m-%d") if date_str else None

#         period1_from_date = parse_date(period1_from)
#         period1_to_date = parse_date(period1_to)
#         period2_from_date = parse_date(period2_from)
#         period2_to_date = parse_date(period2_to)

#         # Conditions for query
#         conditions = search_PeriodComparison_controller()
#         if not isinstance(conditions, list):
#             conditions = []

#         sales_data = {
#             "store_name": set(),
#             "sale_type": set(),
#             "city": set(),
#             "section": set(),
#             "item_category": set(),
#             "product_group": set(),
#             "brand_name": set(),
#             "item_description": set(),
#             "model_no": set(),
#             "demo_flag": set(),
#         }

#         # Helper function to fetch and merge data
#         def fetch_and_merge_data(from_date, to_date):
#             if from_date and to_date:
#                 period_conditions = conditions + [
#                     SalesAllInOneLive.invoice_date.between(from_date, to_date)
#                 ]
#                 records = SalesAllInOneLive.query.filter(*period_conditions).all()  # Unpack conditions correctly
#                 for record in records:
#                     sales_data["store_name"].add(record.store_name)
#                     sales_data["sale_type"].add(record.sale_type)
#                     sales_data["section"].add(record.section)
#                     sales_data["item_category"].add(record.item_category)
#                     sales_data["product_group"].add(record.product_group)
#                     sales_data["brand_name"].add(record.brand_name)
#                     sales_data["item_description"].add(record.item_description)
#                     sales_data["model_no"].add(record.model_no)
#                     sales_data["city"].add(record.city)
#                     sales_data["demo_flag"].add(record.demo_flag)

#         # Fetch data for period1
#         if period1_from_date and period1_to_date:
#             fetch_and_merge_data(period1_from_date, period1_to_date)
#         elif period2_from_date and period2_to_date:
#             fetch_and_merge_data(period2_from_date, period2_to_date)

#         # Fetch data for period2
#         if period2_from_date and period2_to_date:
#             fetch_and_merge_data(period2_from_date, period2_to_date)
#         elif period1_from_date and period1_to_date:
#             fetch_and_merge_data(period1_from_date, period1_to_date)

#         # Convert sets to lists for JSON serialization
#         sales_data = {key: list(value) for key, value in sales_data.items()}

#         return jsonify(sales_data)

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_PeriodComparison_all_in_column_live_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})


def get_PeriodComparison_kore_all_in_column_live_controller():
    try:

          
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        srn_flag = request.args.get('srn_flag')
        sales_type = request.args.get('sales_type')
        section = request.args.get('section')
        # product_group = request.args.get('product_group')
        # item_category = request.args.get('item_category')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        item_description = request.args.get('item_description')
        # store_name = request.args.get('store_name')
        # city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        # asm = request.args.get('asm')
        store_code = request.args.get('storecode')

        # Initialize conditions
        conditions = []
        price_conditions = []

        # Apply dynamic conditions
        # if asm:
        #     conditions.append(SalesAllInOneLive.asm == asm)
        if store_code:
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))
        if period_from:
            conditions.append(SalesAllInOneLive.invoice_date >= period_from)
        if period_to:
            conditions.append(SalesAllInOneLive.invoice_date <= period_to)
        if invoice_date:
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        if srn_flag:
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))
        if sales_type:
            sale_types_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_types_list))
        if section:
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))
        # if product_group:
        #     product_group_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', product_group) if isinstance(product_group, str) else product_group
        #     conditions.append(SalesAllInOneLive.product_group.in_(product_group_list))
        # if item_category:
        #     item_category_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_category) if isinstance(item_category, str) else item_category
        #     conditions.append(SalesAllInOneLive.item_category.in_(item_category_list))
        if brand_name:
            brand_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', brand_name) if isinstance(brand_name, str) else brand_name
            conditions.append(SalesAllInOneLive.brand_name.in_(brand_name_list))
        if model_no:
            model_no_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', model_no) if isinstance(model_no, str) else model_no
            conditions.append(SalesAllInOneLive.model_no.in_(model_no_list))
        if item_description:
            item_description_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', item_description) if isinstance(item_description, str) else item_description
            conditions.append(SalesAllInOneLive.item_description.in_(item_description_list))
        # if store_name:
        #     store_name_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_name) if isinstance(store_name, str) else store_name
        #     conditions.append(SalesAllInOneLive.store_name.in_(store_name_list))
        # if city:
        #     city_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', city) if isinstance(city, str) else city
        #     conditions.append(SalesAllInOneLive.city.in_(city_list))
        if demo_flag:
            demo_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', demo_flag) if isinstance(demo_flag, str) else demo_flag
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
            # SalesAllInOneLive.store_name,
            # SalesAllInOneLive.city,
            SalesAllInOneLive.section,
            SalesAllInOneLive.item_description,
            # SalesAllInOneLive.product_group,
            SalesAllInOneLive.brand_name,
            SalesAllInOneLive.model_no,
            SalesAllInOneLive.demo_flag,
            SalesAllInOneLive.srn_flag,
            SalesAllInOneLive.sale_type,
            # SalesAllInOneLive.item_category,
        ]

        # Perform optimized query with distinct aggregation
        query = db.session.query(*columns).filter(*conditions).distinct()

        # Utilize server-side pagination for massive data (optional)
        results = query.all()  # Adjust the limit as needed

        # Process the results dynamically
        sales_data = {}
        for column in columns:
            sales_data[column.key] = set()

        for record in results:
            for column, value in zip(columns, record):
                sales_data[column.key].add(value)

        # Convert sets to lists for JSON serialization
        sales_data = {key: list(value) for key, value in sales_data.items()}

        return jsonify(sales_data)

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return get_PeriodComparison_kore_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

