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

@app.route('/querydatabase/<server>/<database>/<username>/<password>/<query>')
def query_database(server, database, username, password, query):
    return sellers.query_database(server, database, username, password, query)

## Note to self