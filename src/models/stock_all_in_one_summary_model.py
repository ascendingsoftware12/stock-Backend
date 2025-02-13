from src import db
from flask_sqlalchemy import SQLAlchemy

class StockAllInOneSummary(db.Model):
    __tablename__ = 'stock'

    id = db.Column(db.Integer,primary_key=True)
    branch_code = db.Column(db.String(255), nullable=True)
    branch_name = db.Column(db.String(255), nullable=False)
    item_code = db.Column(db.String(255), nullable=True)
    item_name = db.Column(db.String(255), nullable=True)
    item_category = db.Column(db.String(255), nullable=True)
    brand = db.Column(db.String(255), nullable=True)
    imei_serial_no = db.Column(db.String(255), nullable=True)
    imei_status = db.Column(db.String(255), nullable=True)
    first_inward_date = db.Column(db.Date)
    orig_purchase_rate = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    purchase_rate = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    qty= db.Column('QTY', db.Numeric(53, 0), nullable=True)
    supp_internal_code = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    overall_age= db.Column('Overall_age', db.Numeric(53, 0), nullable=True)
    selling_price = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    cost_price = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    landing_price = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    product = db.Column(db.String(60), nullable=True)
    section = db.Column(db.String(255), nullable=True)
    modelno = db.Column(db.String(255), nullable=True)
    actual_item = db.Column(db.String(255), nullable=True)
    city = db.Column(db.String(255), nullable=True)
    state = db.Column(db.String(255), nullable=True)
    region = db.Column(db.String(255), nullable=True)
    store_openend_on = db.Column('STORE_OPENED_ON', db.Numeric(53, 0), nullable=True)
    brgstn = db.Column(db.String(20), nullable=True)
    store_category = db.Column(db.String(255), nullable=True)
    rsm = db.Column(db.String(255), nullable=True)
    arsm = db.Column(db.String(255), nullable=True)
    asm = db.Column(db.String(255), nullable=True)
    drsm = db.Column(db.String(255), nullable=True)
    care_taker = db.Column(db.String(255), nullable=True)
    teamleader = db.Column(db.String(255), nullable=True)
    store_closed = db.Column(db.String(1), nullable=True)
    store_closed_on= db.Column('STORE_CLOSED_ON', db.Numeric(53, 0), nullable=True)
    demo_flag = db.Column(db.String(1), default='N', nullable=True)
    holding_cost = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    data_refresh_time = db.Column('DATA_REFRESH_TIME', db.DateTime, nullable=True)













    
