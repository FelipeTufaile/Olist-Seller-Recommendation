from flask import Flask
from flask import request
import sellers

app = Flask(__name__)

@app.route('/geolocation/<query>/<api_key>')
def get_geolocation(query, api_key):
    return sellers.get_geolocation(query, api_key)

@app.route('/geodistance/<orgn_lat>/<orgn_lng>/<dest_lat>/<dest_lng>/<api_key>')
def get_seller_customer_distance(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key):
    return sellers.get_seller_customer_distance(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key)

@app.route('/customerprofile/<server>/<database>/<username>/<password>/<customer_id>')
def get_customer_profile(server, database, username, password, customer_id):
    return sellers.get_customer_profile(server, database, username, password, customer_id)

## Note to self