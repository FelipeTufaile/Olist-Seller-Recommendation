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

@app.route('/recommendsellers/<server>/<database>/<username>/<password>/<customer_id>/<product_category>/<payment_installments>/<payment_boleto>/<payment_credit_card>/<payment_voucher>/<payment_debit_card>/<api_key>/<priority_delivery>/<priority_review>/<priority_freight>/<cep>')
def main_function(server, database, username, password, customer_id, product_category, payment_installments, payment_boleto, 
                  payment_credit_card, payment_voucher, payment_debit_card, api_key, priority_delivery, priority_review, 
                  priority_freight, cep):
    return sellers.main_function(server, database, username, password, customer_id, product_category, payment_installments, payment_boleto, 
                                 payment_credit_card, payment_voucher, payment_debit_card, api_key, priority_delivery, priority_review, 
                                 priority_freight, cep)
