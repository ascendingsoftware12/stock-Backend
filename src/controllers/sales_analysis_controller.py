from flask import request, jsonify
from sqlalchemy import case, func,distinct,or_
from src import db
from src.models.sales_all_in_one_live_model import SalesAllInOneLive
from datetime import datetime
import traceback
import re


def search_SalesAnalysis_common_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        

        return conditions
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesAnalysis_common_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


def get_SalesAnlaysis_all_in_column_live_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
            return get_SalesAnlaysis_all_in_column_live_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_sales_type_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        sale_type_query = db.session.query(
            SalesAllInOneLive.sale_type.label('sale_type'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales')
        ).filter(*conditions).group_by(SalesAllInOneLive.sale_type)

        sale_type_result = sale_type_query.all()

        sale_type_result_data = []
        for row in sale_type_result:
            sale_type_result_data.append({
                "sale_type": row.sale_type,
                "total_sales":int(row.total_sales) 
            })

        return jsonify({
            "success": 1,
            "data": sale_type_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_sales_type_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContribution_sales_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        sale_type_query = db.session.query(
            func.sum(SalesAllInOneLive.total_sales).label('total_sales')
        ).filter(*conditions)

        total_sales = sale_type_query.scalar()

        if total_sales is None:
            total_sales_in_cr = 0  
        else:
            
            total_sales_in_cr = total_sales / 10_000_000  

        formatted_sales_in_cr = f"{total_sales_in_cr:.2f} cr"

        sale_type_result_data = {
            "total_sales_in_cr": formatted_sales_in_cr
        }

        return jsonify({
            "success": 1,
            "data": sale_type_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_SalesContribution_sales_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})

def search_SalesContributuion_section_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        total_sales_overall = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar()
        section_query = db.session.query(
            SalesAllInOneLive.section.label('section'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_overall * 100).label('sales_percentage'),
        ).filter(*conditions).group_by(SalesAllInOneLive.section).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        section_result = section_query.limit(limit).offset(offset).all()

        section_result_data = []

        for row in section_result:
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            section_result_data.append({
                "section": row.section,
                "total_sales": int(row.total_sales),
                "asp": int(asp),
                "total_sales_percentage":f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": section_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_section_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_item_category_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        total_sales_overall = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar()

        item_category_query = db.session.query(
            SalesAllInOneLive.item_category.label('item_category'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_overall * 100).label('sales_percentage'),
        ).filter(*conditions).group_by(SalesAllInOneLive.item_category).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        item_category_result = item_category_query.limit(limit).offset(offset).all()

        item_category_result_data = []

        for row in item_category_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            item_category_result_data.append({
                "item_category": row.item_category,
                "total_sales": int(row.total_sales),
                "asp": asp,
                "total_sales_percentage":f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": item_category_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_item_category_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_product_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        total_sales_overall = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar()
        product_group_query = db.session.query(
            SalesAllInOneLive.product_group.label('product_group'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_overall * 100).label('sales_percentage'),
        ).filter(*conditions).group_by(SalesAllInOneLive.product_group).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        product_group_result = product_group_query.limit(limit).offset(offset).all()

        item_category_result_data = []

        for row in product_group_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            item_category_result_data.append({
                "product_group": row.product_group,
                "total_sales": int(row.total_sales),
                "asp": asp,
                "total_sales_percentage":f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": item_category_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_product_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_branch_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        total_sales_overall = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar()
        total_qty_overall = db.session.query(func.sum(SalesAllInOneLive.sales_qty)).filter(*conditions).scalar()
         
        store_name_query = db.session.query(
            SalesAllInOneLive.store_name.label('store_name'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_overall * 100).label('sales_percentage'),
            (func.sum(SalesAllInOneLive.sales_qty) / total_qty_overall * 100).label('sales_qty_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.store_name).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        store_name_result = store_name_query.limit(limit).offset(offset).all()

        store_name_result_data = []

        for row in store_name_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            store_name_result_data.append({
                "store_name": row.store_name,
                "total_sales": int(row.total_sales),  
                "sales_qty": int(row.sales_qty),      
                "sales_qty_percentage": f"{row.sales_qty_percentage:.2f}%",
                "asp": asp,
                "total_sales_percentage": f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": store_name_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_branch_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_city_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        total_sales_overall = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar()
        total_qty_overall = db.session.query(func.sum(SalesAllInOneLive.sales_qty)).filter(*conditions).scalar()

        city_query = db.session.query(
            SalesAllInOneLive.city.label('city'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_overall * 100).label('sales_percentage'),
            (func.sum(SalesAllInOneLive.sales_qty) / total_qty_overall * 100).label('sales_qty_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.city).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        city_result = city_query.limit(limit).offset(offset).all()

        city_result_data = []

        for row in city_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            city_result_data.append({
                "city": row.city,
                "total_sales": int(row.total_sales),
                "sales_qty": int(row.sales_qty),
                "sales_qty_percentage": f"{row.sales_qty_percentage:.2f}%",
                "asp": asp,
                "total_sales_percentage": f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": city_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_city_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_brand_sales_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        total_sales_subq = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar_subquery()
        total_sales_qty_subq = db.session.query(func.sum(SalesAllInOneLive.sales_qty)).filter(*conditions).scalar_subquery()

        brand_name_query = db.session.query(
            SalesAllInOneLive.brand_name.label('brand_name'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_subq * 100).label('sales_percentage'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.sales_qty) / total_sales_qty_subq * 100).label('sales_qty_percentage'),
            (func.sum(SalesAllInOneLive.total_sales) / func.sum(SalesAllInOneLive.sales_qty)).label('ASP')
        ).filter(*conditions).group_by(SalesAllInOneLive.brand_name).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        brand_name_result = brand_name_query.limit(limit).offset(offset).all()

        brand_name_result_data = []

        for row in brand_name_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            brand_name_result_data.append({
                "brand_name": row.brand_name,
                "total_sales": int(row.total_sales),
                "sales_qty": int(row.sales_qty),
                "sales_qty_percentage":f"{row.sales_qty_percentage:.2f}%",
                "asp": asp,
                "total_sales_percentage":f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": brand_name_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_brand_sales_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_SalesContributuion_item_sales_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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

        total_sales_subq = db.session.query(func.sum(SalesAllInOneLive.total_sales)).filter(*conditions).scalar_subquery()
        total_sales_qty_subq = db.session.query(func.sum(SalesAllInOneLive.sales_qty)).filter(*conditions).scalar_subquery()
        
        actual_item_query = db.session.query(
            SalesAllInOneLive.actual_item.label('actual_item'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.total_sales) / total_sales_subq * 100).label('sales_percentage'),
            func.sum(SalesAllInOneLive.sales_qty).label('sales_qty'),
            (func.sum(SalesAllInOneLive.sales_qty) / total_sales_qty_subq * 100).label('sales_qty_percentage'),
        ).filter(*conditions).group_by(SalesAllInOneLive.actual_item).order_by(func.sum(SalesAllInOneLive.total_sales).desc())

        actual_item_result = actual_item_query.limit(limit).offset(offset).all()
        actual_item_result_data = []

        for row in actual_item_result:
            
            asp = round(row.total_sales / row.sales_qty) if row.sales_qty else 0

            actual_item_result_data.append({
                "actual_item": row.actual_item,
                "total_sales": int(row.total_sales),
                "sales_qty": int(row.sales_qty),
                "sales_qty_percentage":f"{row.sales_qty_percentage:.2f}%",
                "asp": asp,
                "total_sales_percentage":f"{row.sales_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": actual_item_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_SalesContributuion_item_sales_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_branch_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        store_name_query = db.session.query(
            SalesAllInOneLive.store_name.label('store_name'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt'),
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.store_name).order_by(func.sum(SalesAllInOneLive.disc_amt).desc())

        branch_result = store_name_query.limit(limit).offset(offset).all()

        
        
        branch_sum = sum(row.disc_amt for row in branch_result)
        branch_result_data = []

        for row in branch_result:
            
           
            disc_amt_percentage = (row.disc_amt / branch_sum) * 100 if branch_sum else 0
            disc_amt_percentage_str = f"{row.discount_percentage:.2f}%"

            branch_result_data.append({
                "store_name": row.store_name,
                "disc_amt": int(row.disc_amt),
                "total_sales": int(row.total_sales),            
                "disc_amt_percentage":f"{row.discount_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": branch_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_branch_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})


    try:
        # Query data grouped by sale_type and including sales_qty
        city_query = db.session.query(
            SalesAllInOneLive.city.label('city'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt')
        ).group_by(SalesAllInOneLive.city)

        # Fetch results
        city_result = city_query.all()

        # Calculate total sales percentage and ASP
        
        city_sum = sum(row.disc_amt for row in city_result)
        city_result_data = []

        for row in city_result:
            # Calculate ASP
            asp = round(row.total_sales / row.disc_amt) if row.disc_amt else 0
            # Calculate sales percentage
           
            disc_amt_percentage = (row.disc_amt / disc_amt_sum) * 100 if disc_amt_sum else 0
            disc_amt_percentage_str = f"{disc_amt_percentage:.2f}%"

            city_result_data.append({
                "city": row.city,
                "disc_amt": row.disc_amt,
                "total_sales": row.total_sales,
            
                "disc_amt_percentage":disc_amt_percentage_str
            })

        return jsonify({
            "success": 1,
            "data": city_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_city_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_city_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        city_query = db.session.query(
            SalesAllInOneLive.city.label('city'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt'),
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.city).order_by(func.sum(SalesAllInOneLive.disc_amt).desc())

        city_result = city_query.limit(limit).offset(offset).all()

        total_sales_sum = sum(row.total_sales for row in city_result)
        disc_amt_sum = sum(row.disc_amt for row in city_result)
        city_result_data = []

        for row in city_result:

            city_result_data.append({
                "city": row.city,
                "total_sales": int(row.total_sales),
                "disc_amt": int(row.disc_amt),
                "disc_amt_percentage":f"{row.discount_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": city_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_city_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_section_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        section_query = db.session.query(
            SalesAllInOneLive.section.label('section'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt'),
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.section).order_by(func.sum(SalesAllInOneLive.disc_amt).desc())

        section_result = section_query.limit(limit).offset(offset).all()

        
        total_sales_sum = sum(row.total_sales for row in section_result)
        disc_amt_sum = sum(row.disc_amt for row in section_result)
        section_result_data = []

        for row in section_result:
           
            section_result_data.append({
                "section": row.section,
                "total_sales": int(row.total_sales),
                "disc_amt": int(row.disc_amt),
                "disc_amt_percentage":f"{row.discount_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": section_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_section_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_brand_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        brand_name_query = db.session.query(
            SalesAllInOneLive.brand_name.label('brand_name'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt'),
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.brand_name).order_by(func.sum(SalesAllInOneLive.disc_amt).desc())

        brand_name_result = brand_name_query.limit(limit).offset(offset).all()

        
        total_sales_sum = sum(row.total_sales for row in brand_name_result)
        disc_amt_sum = sum(row.disc_amt for row in brand_name_result)
        brand_name_result_data = []

        for row in brand_name_result:
            
            brand_name_result_data.append({
                "brand_name": row.brand_name,
                "total_sales": int(row.total_sales),
                "disc_amt": int(row.disc_amt),
                "disc_amt_percentage":f"{row.discount_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": brand_name_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_brand_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_model_no_controller():
    try:
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 10, type=int)
        offset = (page - 1) * limit
        
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        model_no_query = db.session.query(
            SalesAllInOneLive.model_no.label('model_no'),
            func.sum(SalesAllInOneLive.total_sales).label('total_sales'),
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt'),
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions).group_by(SalesAllInOneLive.model_no).order_by(func.sum(SalesAllInOneLive.disc_amt).desc())

        model_no_result = model_no_query.limit(limit).offset(offset).all()

        
        total_sales_sum = sum(row.total_sales for row in model_no_result)
        disc_amt_sum = sum(row.disc_amt for row in model_no_result)
        model_no_result_data = []

        for row in model_no_result:
            
            model_no_result_data.append({
                "model_no": row.model_no,
                "total_sales": int(row.total_sales),
                "disc_amt": int(row.disc_amt),
                "disc_amt_percentage":f"{row.discount_percentage:.2f}%"
            })

        return jsonify({
            "success": 1,
            "data": model_no_result_data
        })

    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return search_Discount_Analysis_model_no_controller()
        else:
            return jsonify({"success": 0, "error": str(e)})

def search_Discount_Analysis_discount_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        
        disc_amt_query = db.session.query(
            func.sum(SalesAllInOneLive.disc_amt).label('disc_amt')
        ).filter(*conditions)

        
        disc_amt = disc_amt_query.scalar()

        
        if disc_amt is None:
            disc_amt_in_cr = 0  
        else:
            
            disc_amt_in_cr = disc_amt / 10000000  

        
        formatted_disc_amt_in_cr = f"{round(disc_amt_in_cr, 2)} Cr"

        
        disc_amt_result_data = {
            "Disc_amt_in_cr": formatted_disc_amt_in_cr
        }

        return jsonify({
            "success": 1,
            "data": disc_amt_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_Discount_Analysis_discount_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})

def search_Discount_Analysis_discount_Percentage_controller():
    try:
        conditions = []
        price_conditions = []
        
        # Fetch filters from request arguments
        period_from = request.args.get("period_from")
        period_to = request.args.get("period_to")
        invoice_date = request.args.get('invoice_date')
        section = request.args.get('section')
        sales_type = request.args.get('sale_type')
        item_category = request.args.get('item_category')
        product_group = request.args.get('product_group')
        brand_name = request.args.get('brand_name')
        model_no = request.args.get('model_no')
        store_name = request.args.get('store_name')
        city = request.args.get('city')
        demo_flag = request.args.get('demo_flag')
        price_breakup2 = request.args.get('PriceBreakup2')
        srn_flag = request.args.get('srn_flag')
        item_description = request.args.get('item_description')
        asm = request.args.get('asm')        
        store_code = request.args.get('storecode')

        if store_code and store_code != '':
            store_code_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', store_code) if isinstance(store_code, str) else store_code
            conditions.append(SalesAllInOneLive.store_code.in_(store_code_list))

        if srn_flag and srn_flag != '':
            srn_flag_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', srn_flag) if isinstance(srn_flag, str) else srn_flag
            conditions.append(SalesAllInOneLive.srn_flag.in_(srn_flag_list))

        if asm and asm != '':
            conditions.append(SalesAllInOneLive.asm == asm)



        if period_from and period_to and period_from != '' and period_to != '':
            conditions.append(SalesAllInOneLive.invoice_date.between(period_from, period_to))

        if invoice_date and invoice_date != '':
            invoice_date_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', invoice_date) if isinstance(invoice_date, str) else invoice_date
            conditions.append(SalesAllInOneLive.invoice_date.in_(invoice_date_list))
        

        if sales_type and sales_type != '':
            sale_type_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', sales_type) if isinstance(sales_type, str) else sales_type
            conditions.append(SalesAllInOneLive.sale_type.in_(sale_type_list))

        if section and section != '':
            section_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', section) if isinstance(section, str) else section
            conditions.append(SalesAllInOneLive.section.in_(section_list))

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
        
        # Handle price breakup conditions
        if price_breakup2:
            sales_per_unit = SalesAllInOneLive.total_sales / SalesAllInOneLive.sales_qty
            price_breakup2_list = re.split(r',(?=(?:[^()]*\([^\)]*\))?[^()]*$)', price_breakup2) if isinstance(price_breakup2, str) else price_breakup2
            
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
        disc_amt_query = db.session.query(
            (func.sum(SalesAllInOneLive.disc_amt) / 
            (func.sum(SalesAllInOneLive.disc_amt) + func.sum(SalesAllInOneLive.total_sales)) * 100)
            .label('discount_percentage')
        ).filter(*conditions)

        disc_amt_sum = disc_amt_query.scalar()
        disc_amt_sum = disc_amt_sum or 0 

        disc_amt_percentage = disc_amt_sum  
        disc_amt_percentage_str = f"{disc_amt_percentage:.2f}%"

        disc_amt_result_data = {
            "disc_amt_percentage": disc_amt_percentage_str
        }

        return jsonify({
            "success": 1,
            "data": disc_amt_result_data
        })

    except Exception as e:
        db.session.rollback()
        error_message = str(e)
        if "MySQL server has gone away" in error_message:
            return search_Discount_Analysis_discount_Percentage_controller()  
        else:
            return jsonify({"success": 0, "error": error_message})



# def search_Discount_Analysis_discount_Percentage_controller():
#     try:
#         # Query to calculate total discount amount
#         disc_amt_query = db.session.query(
#             func.sum(SalesAllInOneLive.disc_amt).label('disc_amt_sum')
#         )
#         disc_amt_sum = disc_amt_query.scalar() or 0  # Handle None by defaulting to 0

#         # Query to calculate total sales amount
#         total_sales_query = db.session.query(
#             func.sum(SalesAllInOneLive.sales_amt).label('total_sales_sum')
#         )
#         total_sales_sum = total_sales_query.scalar() or 1  # Handle None by defaulting to 1 to avoid division by zero

#         # Calculate discount percentage
#         disc_amt_percentage = (disc_amt_sum / total_sales_sum) * 100
#         disc_amt_percentage_str = f"{disc_amt_percentage:.2f}%"  # Format as percentage

#         disc_amt_result_data = {
#             "disc_amt_percentage": disc_amt_percentage_str
#         }

#         return jsonify({
#             "success": 1,
#             "data": disc_amt_result_data
#         })

#     except Exception as e:
#         db.session.rollback()
#         error_message = str(e)
#         if "MySQL server has gone away" in error_message:
#             return search_Discount_Analysis_discount_Percentage_controller()  # Retry logic
#         else:
#             return jsonify({"success": 0, "error": error_message})
