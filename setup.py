from cx_Freeze import setup, Executable
import os

# Replace 'app.py' with your main script file
executables = [Executable('app.py', base='Console')]
os.environ["DB_PORT"] = "3306"  # Set default environment variable value for the port
os.environ["DB_USER"]="root"
os.environ["DB_PASSWORD"]="8520"
os.environ["DB_NAME"]="apx_stock_apps"
os.environ["DB_HOST"]="127.0.0.1"
setup(
    name='FlaskApp',
    version='0.1',
    description='My Flask Application',
    executables=executables,
      
)


