from flask import current_app, request, jsonify
from datetime import datetime, timedelta
from functools import wraps
from src.models.user_model import User
import os
import jwt

def generate_token(username, role):
    try:
        payload = {
            'exp': int((datetime.utcnow() + timedelta(days=1)).timestamp()), 
            'iat': int(datetime.utcnow().timestamp()),  
            'username': username,
            'role': role
        }
        JWT_SECRET_KEY =  os.getenv('SECRET_KEY')
        token = jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')
        return token
    except Exception as e:
        return str(e)

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]

        if not token:
            return jsonify({'message': 'token is missing'}), 401

        try:
            JWT_SECRET_KEY =  os.getenv('SECRET_KEY')
            token_data = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            current_user = User.query.filter_by(username=token_data['username']).first()
            
            if not current_user:
                return jsonify({'message': 'User not found'}), 404
            
            print(current_user)
            return f(current_user, token_data['role'], *args, **kwargs)
        
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
        except Exception as e:
            return jsonify({'error': str(e)}), 401

    return decorated
