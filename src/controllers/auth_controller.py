from flask import request, make_response, jsonify
import bcrypt
import os
import jwt
import pandas as pd

from src import db
from src.models.user_model import User
from src.utils.jwt_token_utils import generate_token


def register_user_controller():
    try:
        data = request.get_json()
        username = data.get("username")
        password = data.get("password")
        role = data.get("role", "user")

        if not username or not password:
            return (
                jsonify({"message": "Username and password required", "status": 0}),
                400,
            )

        if User.query.filter_by(username=username).first():
            return jsonify({"message": "User already exists", "status": 2}), 400

        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

        new_user = User(
            username=username,
            password=hashed_password.decode("utf-8"),
            role=role,
        )

        db.session.add(new_user)
        db.session.commit()

        return jsonify({"message": "User registered successfully", "status": 1}), 201
    
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return register_user_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


def login_user_controller():
    try:
        data = request.get_json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return (
                jsonify({"message": "Username and password required", "status": 0}),
                400,
            )

        user = User.query.filter_by(username=username).first()

        if not user or not bcrypt.checkpw(
            password.encode("utf-8"), user.password.encode("utf-8")
        ):
            return jsonify({"message": "Invalid credentials", "status": 0}), 401

        token = generate_token(user.username, user.role)
        return (
            jsonify(
                {
                    "message": "Login successful",
                    "token": token,
                    "username": user.username,
                    "role": user.role,
                    "status": 1,
                }
            ),
            200,
        )
    except Exception as e:
        db.session.rollback()
        if "MySQL server has gone away" in str(e):
            return login_user_controller()
        else:
            return (jsonify({"success": 0, "error": str(e)}), 500)


# blacklisted_tokens = set()
# def logout_user_controller():
#     try:
#         auth_header = request.headers.get('Authorization')
#         if auth_header and auth_header.startswith('Bearer '):
#             token = auth_header.split(' ')[1]
#             blacklisted_tokens.add(token)
#             return jsonify({"message": "Logged out successfully", "status": 1}), 200
#         return jsonify({"message": "Logged out successfully", "status": 0}), 401
#     except jwt.ExpiredSignatureError:
#         return jsonify({"error": "Token has expired", "status": 0}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({"error": "Invalid token", "status": 0}), 401
#     except Exception as e:
#         return jsonify({"error": str(e), "status": 0}), 401


# def token_is_blacklisted(token):
#     return token in blacklisted_tokens


def import_users_from_csv():
    try:
        file_path = r"D:\PROGRAM\Flask_APIs\APIs\stock_optimization\src\controllers\userdata.csv"
        df = pd.read_csv(file_path)

        for index, row in df.iterrows():
            username = row['name']
            password = row['PASSWORD']
            
            if User.query.filter_by(username=username).first():
                print(f"User {username} already exists, skipping...")
                continue
            
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            
            new_user = User(
                username=username,
                password=hashed_password.decode('utf-8'),
                role='user'
            )
            
            db.session.add(new_user)

        db.session.commit()
        return (jsonify({"success": 1, "message": "Users imported"}), 200)

    except Exception as e:
        db.session.rollback()
        return (jsonify({"success": 0, "error": str(e)}), 500)
        
