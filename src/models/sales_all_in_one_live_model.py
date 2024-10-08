from src import db

class SalesAllInOneLive(db.Model):
    __tablename__ = 'sales_all_in_one_live'
    
    id = db.Column(db.Integer,primary_key=True)
    invoice_date = db.Column(db.Date)
    store_code = db.Column(db.String(10), nullable=True)
    store_name = db.Column(db.String(60), nullable=False)
    city = db.Column(db.String(255), nullable=True)
    region = db.Column(db.String(255), nullable=True)
    state = db.Column(db.String(255), nullable=True)
    item_code = db.Column(db.String(20), nullable=True)
    item_description = db.Column(db.String(255), nullable=True)
    actual_item = db.Column(db.String(255), nullable=True)
    brand_name = db.Column(db.String(255), nullable=True)
    product_group = db.Column(db.String(60), nullable=False)
    section = db.Column(db.String(60), nullable=True)
    model_no = db.Column('MODELNO', db.String(255), nullable=True)
    sales_qty = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    gros_rate = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    disc_amt = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    tax_amt = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    total_sales = db.Column(db.Float(precision=19, decimal_return_scale=2), nullable=True)
    cost_price = db.Column('COST_PRICE', db.Numeric(53, 0), nullable=True)
    gros_profit = db.Column('GROS_PROFIT', db.Numeric(53, 2), nullable=True)
    revealer_section = db.Column(db.String(45), nullable=True)
    store_category = db.Column('STORE_CATEGORY', db.String(255), nullable=True)
    store_opened_date = db.Column('Store_Opened_Date', db.Date, nullable=True)
    rsm = db.Column('RSM', db.String(255), nullable=True)
    arsm = db.Column('ARSM', db.String(255), nullable=True)
    asm = db.Column('ASM', db.String(255), nullable=True)
    drsm = db.Column('DRSM', db.String(255), nullable=True)
    branch_type = db.Column('BRANCH_TYPE', db.String(255), nullable=True)
    franch_type = db.Column('FRANCH_TYPE', db.String(255), nullable=True)
    srn_flag = db.Column(db.String(255), nullable=True)
    sale_type = db.Column('SALE_TYPE', db.String(4), nullable=True)
    demo_flag = db.Column(db.String(1), default='N', nullable=True)
    data_refresh_time = db.Column('Data_Refresh_Time', db.DateTime, nullable=True)





# ----------------------------------------------------------------------------------------------------------
# template for empty controller
# ----------------------------------------------------------------------------------------------------------


# def get_sales_all_in_one_live_ytd_controller():
#     try:
#         pass

#     except Exception as e:
#         db.session.rollback()
#         if "MySQL server has gone away" in str(e):
#             return get_sales_all_in_one_live_ytd_controller()
#         else:
#             return jsonify({"success": 0, "error": str(e)})


# ----------------------------------------------------------------------------------------------------------