from src import db
from sqlalchemy.sql import func
from flask_sqlalchemy import SQLAlchemy

class TargetAchievementAnalysisEmpRanking(db.Model):
    __tablename__ = 'emp_ranking'

    id = db.Column(db.Integer,primary_key=True)
    tgt_period = db.Column(db.String(255), nullable=True)
    tgt_timeline = db.Column(db.String(255), nullable=True)
    emp_code = db.Column(db.String(10), nullable=True)
    emp_name = db.Column(db.String(255), nullable=True)
    section = db.Column(db.String(255), nullable=True)
    total_qty = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    total_sales = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    total_disc = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    target= db.Column('TARGET', db.Numeric(53, 0), nullable=True)
    target_ach_percent = db.Column('target_ach%', db.Float(precision=23, decimal_return_scale=6), nullable=True)
    target_ach = db.Column(db.String(255), nullable=True)
    targe_ach_flag= db.Column('TARGET_ACH_FLAG', db.Numeric(53, 0), nullable=True)
    overall_target_acheived= db.Column('OVERALL_TARGET_ACHEIVED', db.Numeric(53, 0), nullable=True)
    last_3mth_ach_count= db.Column('LAST_3MTH_ACH_COUNT', db.Numeric(53, 0), nullable=True)
    target_lm_ach_percentage = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    target_mb_ach_percentage = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    target_2mb_ach_percentage = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    target_3mth_avg_ach_percentage = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    avg_val_sold_per_day = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    no_of_days= db.Column('NO_OF_DAYS', db.Numeric(53, 0), nullable=True)
    days_remaining= db.Column('DAYS_REMAINING', db.Numeric(53, 0), nullable=True)
    avg_value_ach_target = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    proj_target = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    cm_3mnth_growth = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    cm_lm_growth = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    sales_proj = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    disc_proj = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    tgt_type = db.Column(db.String(255), nullable=True)
    tgt_percentage = db.Column(db.Float(precision=23, decimal_return_scale=6), nullable=True)
    tgt_section_count= db.Column('TGT_SECTION_COUNT', db.Numeric(53, 0), nullable=True)
    tgt_sno= db.Column('TGT_SNO', db.Numeric(53, 0), nullable=True)
    city = db.Column(db.String(255), nullable=True)
    state = db.Column(db.String(255), nullable=True)
    region = db.Column(db.String(255), nullable=True)
    store_opened_on = db.Column(db.Date)
    store_category = db.Column(db.String(255), nullable=True)
    asm = db.Column(db.String(255), nullable=True)
    care_taker = db.Column(db.String(255), nullable=True)
    data_update_time = db.Column(db.Date)
    data_update_time = db.Column(db.TIMESTAMP, server_default=func.now(), onupdate=func.now())










