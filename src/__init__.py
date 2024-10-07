from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from dotenv import load_dotenv
import os

# Init db
db = SQLAlchemy()
SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True, 
        "pool_recycle": 300,
    }

def create_app():
    # Load env variables
    load_dotenv()

    # Create app
    app = Flask(__name__)

    # Handling CORS
    CORS(app)
    app.config['CORS_HEADERS'] = 'Content-Type'
    # Config section
    app.config["SQLALCHEMY_DATABASE_URI"] = (
        "mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}".format(
            DB_USER=os.getenv("DB_USER"),
            DB_PASSWORD=os.getenv("DB_PASSWORD"),
            DB_HOST=os.getenv("DB_HOST"),
            DB_NAME=os.getenv("DB_NAME"),
            pool_pre_ping=True, 
            pool_recycle=300
        )
    )
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Connect the db to app
    db.init_app(app)

    # Init migration
    migrate = Migrate(app, db)

    # Import routes
    # headoffice
    from src.routes.approve_transfer_routes import approve_transfer_bp
    from src.routes.transfer_report_routes import transfer_report_bp
    from src.routes.stock_analysis_routes import stock_analysis_bp
    from src.routes.procurement_routes import procurement_bp

    # store
    from src.routes.transfer_summary_routes import transfer_summary_bp
    from src.routes.transfer_receive_summary_routes import transfer_receive_summary_bp

    # shared
    from src.routes.stock_summary_routes import stock_summary_bp
    from src.routes.target_monitoring_routes import target_monitoring_bp

    # section
    from src.routes.category_routes import category_bp
    from src.routes.auth_route import auth_bp

    from src.routes.sales_all_in_one_live_routes import sales_all_in_one_live_bp


    app.register_blueprint(approve_transfer_bp)
    app.register_blueprint(transfer_report_bp)
    app.register_blueprint(stock_analysis_bp)
    app.register_blueprint(procurement_bp)
    app.register_blueprint(transfer_summary_bp)
    app.register_blueprint(transfer_receive_summary_bp)
    app.register_blueprint(stock_summary_bp)
    app.register_blueprint(target_monitoring_bp)
    app.register_blueprint(category_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(sales_all_in_one_live_bp)

    return app
