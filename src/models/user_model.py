from src import db

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(300), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='user')
    asmrole = db.Column(db.String(45), nullable=True)
    storerole = db.Column(db.String(45), nullable=True)