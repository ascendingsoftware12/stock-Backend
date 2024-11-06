from src import create_app
from waitress import serve
# Start the app
app = create_app()

if __name__ == "__main__":
    # # serve(app, host='182.16.16.27', port=7965,threads=12) #pykoredeployment
    # serve(app, host='192.168.0.223', port=7965,threads=12) #TCM deployment
    
    # app.run(host='182.16.16.28',port='5000',debug=True)    
    app.run(debug=True)