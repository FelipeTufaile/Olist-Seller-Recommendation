from flask import Flask
from flask import request
import sellers

app = Flask(__name__)

@app.route('/geolocation/<query>/<api_key>')
def get_geolocation(query, api_key):
    return sellers.get_geolocation(query, api_key)