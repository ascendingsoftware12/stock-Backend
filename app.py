from src import create_app
from waitress import serve
# Start the app
app = create_app()

if __name__ == "__main__":
    serve(app, host='182.16.16.27', port=7965,threads=12) #pykoredeployment
    # app.run(host='182.16.16.28',port='5000',debug=True)
    # app.run(debug=True)
    # app.run(debug=True)