from flask import Flask
from flask import request
import sellers

app = Flask(__name__)

@app.route('/geolocation/<query>/<api_key>')
def get_geolocation(query, api_key):
    return sellers.get_geolocation(query, api_key)

@app.route('/geolocation/<orgn_lat>/<orgn_lng>/<dest_lat>/<dest_lng>/<api_key>')
def get_seller_customer_distance(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key):
    return sellers.get_geolocation(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key)