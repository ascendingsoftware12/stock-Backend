from src import db

class GeneralBranchRankingModel(db.Model):
    __tablename__ = "general_branch_ranking"
    ID = db.Column(db.Integer,primary_key=True)
    STAT_YEAR = db.Column(db.String(20), nullable=False)
    Ddate = db.Column(db.String(30), nullable=False)
    MYear = db.Column(db.Integer, nullable=False)
    Mmonth = db.Column(db.Integer, nullable=False)
    store_code = db.Column(db.String(50), nullable=False)
    store_name = db.Column(db.String(50), nullable=False)
    Brand_name = db.Column(db.String(50), nullable=True)
    section = db.Column(db.String(20), nullable=False)
    TOTAL_QTY = db.Column(db.Integer, nullable=False)
    TOTAL_SALES = db.Column(db.Integer, nullable=False)
    target = db.Column(db.Integer, nullable=False)
    Target_Ach_percentage = db.Column(db.Float, nullable=False)
    Target_Ach = db.Column(db.String(50), nullable=False)
    Target_ach_flag = db.Column(db.Integer, nullable=False)
    Overall_Target_Acheived = db.Column(db.Integer, nullable=False)
    region = db.Column(db.String(50), nullable=True)
    city = db.Column(db.String(50), nullable=False)
    store_location = db.Column(db.String(50), nullable=True)
    store_Category = db.Column(db.String(50), nullable=True)
    Store_opened_on = db.Column(db.String(50), nullable=False)
    state = db.Column(db.String(50), nullable=True)
    zone = db.Column(db.String(50), nullable=True)
    care_taker = db.Column(db.String(50), nullable=True)
    store_class = db.Column(db.String(50), nullable=True)
    ASM = db.Column(db.String(50), nullable=False)
    Last_3mth_ach_count = db.Column(db.Integer, nullable=False)
    Target_LM_Ach_percentage = db.Column(db.Float, nullable=False)
    Target_MB_Ach_percentage = db.Column(db.Float, nullable=False)
    Target_2MB_Ach_percentage = db.Column(db.Float, nullable=False)
    Target_3mth_avg_Ach_percentage = db.Column(db.Float, nullable=False)
    MEND_DATE = db.Column(db.String(50), nullable=False)
    AVG_VAL_SOLD_PER_DAY = db.Column(db.Float, nullable=False)
    NO_OF_DAYS = db.Column(db.Integer, nullable=False)
    DAYS_REMAINING = db.Column(db.Integer, nullable=False)
    AVG_VALUE_ACH_TARGET = db.Column(db.Float, nullable=False)
    PROJ_TARGET = db.Column(db.Float, nullable=True)
    CM_3MNTH_GROWTH = db.Column(db.Float, nullable=True)
    CM_LM_GROWTH = db.Column(db.Float, nullable=True)
    ACH_RATIO = db.Column(db.Float, nullable=False)
    data_updated_time = db.Column(db.String(50), nullable=False)




    


    

    						
