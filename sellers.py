import requests
from sqlalchemy import create_engine
import urllib
import pandas as pd
import numpy as np


######################################### CREATING FUNCTION: GET_GEOLOCATION #########################################
######################################################################################################################

def get_geolocation(query, api_key):

    # base url
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?"

    # building url
    parameters = "input={query}&inputtype=textquery&fields=formatted_address%2Cgeometry&key={api_key}"
    url = base_url + parameters.format(query=str(query), api_key=str(api_key))

    # define paylodas and headers as empty
    payload={}
    headers = {}

    # make get request and convert response to json
    data = requests.request("GET", url, headers=headers, data=payload).json()

  
    # Parse information
    try:
        address = data['candidates'][0]['formatted_address'] 
    except:
        return {'ERROR! THE RESQUEST PRODUCED NO RESULTS.'}
  
    # Parse information | Latitude
    try:
        latitude = data['candidates'][0]['geometry']['location']['lat']
    except:
        return {'ERROR! THE RESQUEST PRODUCED NO RESULTS.'}
  
    # Parse information | Longitude
    try:
        longitude = data['candidates'][0]['geometry']['location']['lng']
    except:
        return {'ERROR! THE RESQUEST PRODUCED NO RESULTS.'}
  
    return {'location_address':address, 'location_latitude':latitude, 'location_longitude':longitude}


################################### CREATING FUNCTION: get_seller_customer_distance ##################################
######################################################################################################################

## Creating function calculate seller customer distance
def get_seller_customer_distance(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key):

    #return {'orgn_lat':orgn_lat, 'orgn_lng':orgn_lng}
  
    # building url
    base_url = "https://maps.googleapis.com/maps/api/distancematrix/json?"
    parameters = "origins={orgn_lat}%2C{orgn_lng}&destinations={dest_lat}%2C{dest_lng}&key={api_key}"
    url = base_url + parameters.format(orgn_lat=orgn_lat, orgn_lng=orgn_lng, dest_lat=dest_lat, dest_lng=dest_lng, api_key=str(api_key))
  
    # define paylodas and headers as empty
    payload={}
    headers = {}
  
    # make get request
    response = requests.request("GET", url, headers=headers, data=payload)

    # convert response to json
    data = response.json()
  
    # Parse response | Customet to Seller distance
    try:
        travel_distance_km = data['rows'][0]['elements'][0]['distance']['value']/1000
    except:
        return {'ERROR! THE RESQUEST PRODUCED NO RESULTS.'}
  
    # Parse response | Customet to Seller driving duration
    try:
        duration_hours = data['rows'][0]['elements'][0]['duration']['value']/3600
    except:
        return {'ERROR! THE RESQUEST PRODUCED NO RESULTS.'}

    return {"travel_distance_km":travel_distance_km, "duration_hours":duration_hours}


############################################ CREATING FUNCTION: db_connect ###########################################
######################################################################################################################

def db_connect(server, database, username, password):

    # read credentials
    driver = '{ODBC Driver 18 for SQL Server}'
    conn_mode = 'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'

    # create connection object
    conn_obj = r'Driver={driver};Server=tcp:{server},1433;Database={database};Uid={username};Pwd={password};{conn_mode}'

    # create connection string
    conn_str = conn_obj.format(driver=driver, server=server, database=database, username=username, password=password, conn_mode=conn_mode)

    # parse using urllib
    conn_url = 'mssql+pyodbc:///?odbc_connect={url}'.format(url=urllib.parse.quote_plus(conn_str))

    # create engine
    engine_azure = create_engine(conn_url,echo=False)

    return engine_azure

########################################## CREATING FUNCTION: query_database #########################################
######################################################################################################################

def get_customer_profile(server, database, username, password, customer_id):

    # Start a connection using your dredentials
    database = db_connect(server, database, username, password)

    # Building query
    query = "SELECT * FROM dbo.tb_customers_profile WHERE customer_unique_id = '{customer_id}'".format(customer_id=customer_id)

    # Get columns
    columns = database.execute(query).keys()

    # Get data
    result = database.execute(query)

    ## Querying information
    return {'data':[dict(zip(columns, row)) for row in result]}

######################################################################################################################


# Creating function to create customer profile
def get_customer_profile(connection, customer_id, api_key, payment_installments, payment_boleto, payment_credit_card, 
                         payment_voucher, payment_debit_card, cep=''):

    # Building query
    query = "SELECT * FROM dbo.tb_customers_profile WHERE customer_unique_id = '{customer_id}'"
    query = query.format(customer_id=customer_id)

    # Get columns
    columns = connection.execute(query).keys()

    # Get data
    data = connection.execute(query)

    # Convert response to pandas table
    tb_customer_profile = pd.DataFrame(data, columns=columns)


    ## Creating customer array
    if(tb_customer_profile.shape[0] == 0):
    
        # Removing spare columns
        tb_customer_profile = tb_customer_profile.drop(columns=['customer_lat', 'customer_lng'], axis=0)
    
        # Calculating customer info
        location = get_geolocation(query=cep, api_key=api_key)
        address = location['location_address']
        customer_lat = location['location_latitude']
        customer_lng = location['location_longitude']

        # Creating table
        data = np.zeros(tb_customer_profile.shape[1]-1).tolist()
        tb_customer_profile = pd.concat([tb_customer_profile.reset_index(drop=True), 
                                         pd.DataFrame([data], columns=list(columns)[1:-2]).reset_index(drop=True)],
                                         axis=0)
        # Updating payment method and conditions
        tb_customer_profile['payment_installments'] = payment_installments
        tb_customer_profile['payment_boleto'] = payment_boleto
        tb_customer_profile['payment_credit_card'] = payment_credit_card
        tb_customer_profile['payment_voucher'] = payment_voucher
        tb_customer_profile['payment_debit_card'] = payment_debit_card
    
        # Define an empty vector for new customer
        return (tb_customer_profile[tb_customer_profile.columns[1:]].to_numpy(), customer_lat, customer_lng, address)
   
    else:
    
        customer_lat = tb_customer_profile['customer_lat'][0]
        customer_lng = tb_customer_profile['customer_lng'][0]
    
        # Removing spare columns
        tb_customer_profile = tb_customer_profile.drop(columns=['customer_lat', 'customer_lng'], axis=0)

        # Updating payment method and conditions
        tb_customer_profile['payment_installments'] = payment_installments
        tb_customer_profile['payment_boleto'] = payment_boleto
        tb_customer_profile['payment_credit_card'] = payment_credit_card
        tb_customer_profile['payment_voucher'] = payment_voucher
        tb_customer_profile['payment_debit_card'] = payment_debit_card
    
        ## Adjusting values that are inversely proportional in customer table
        tb_customer_profile[tb_customer_profile.columns[1:13]] = tb_customer_profile[tb_customer_profile.columns[1:13]].apply(lambda x:-x)

        return (tb_customer_profile[tb_customer_profile.columns[1:]].to_numpy(), customer_lat, customer_lng, '')

######################################################################################################################

# Define function that creates seller profiles
def get_seller_profile(connection, product_category, dest_lat, dest_lng, api_key):

    # Building query
    query = "SELECT * FROM dbo.tb_sellers_profile WHERE product_category_name = '{product_category}'"
    query = query.format(product_category=product_category)

    # Get columns
    columns = connection.execute(query).keys()

    # Get data
    data = connection.execute(query)

    # Convert response to pandas table
    tb_sellers_profile = pd.DataFrame(data, columns=columns)
    tb_sellers_profile = tb_sellers_profile.drop(columns=['product_category_name'], axis=0)

    # Defining sellers list
    sellers_list = tb_sellers_profile[['seller_id', 'seller_lat', 'seller_lng']].to_dict('records')
    
    # Defining empty list
    lines = []
  
    # Calculating distances between customer and seller
    for seller in sellers_list:
      
        orgn_lat = seller['seller_lat']
        orgn_lng = seller['seller_lng']
        seller_id = seller['seller_id']

        # Parse response
        distances = get_seller_customer_distance(orgn_lat, orgn_lng, dest_lat, dest_lng, api_key)
        distances_tuple = (distances['travel_distance_km'], distances['duration_hours'])
      
        lines.append(distances_tuple)
      
    temp = pd.DataFrame(lines, columns = ['travel_distance_km', 'duration_hours'])
    
    # Updating sellers profile tabble
    tb_sellers_profile['travel_distance_km'] = temp['travel_distance_km']
    tb_sellers_profile['travel_distance_km'] = temp['duration_hours']
    tb_sellers_profile['freight_value'] = tb_sellers_profile['freight_value_km']*temp['travel_distance_km']
  
    # Removing geolocation info
    tb_sellers_profile = tb_sellers_profile.drop(columns=['seller_lat', 'seller_lng'], axis=0)
    
    ## Adjusting values that are inversely proportional in customer table
    tb_sellers_profile[tb_sellers_profile.columns[1:13]] = tb_sellers_profile[tb_sellers_profile.columns[1:13]].apply(lambda x:-x)
    
    return (tb_sellers_profile[tb_sellers_profile.columns[1:]].to_numpy(), list(tb_sellers_profile['seller_id']))


######################################################################################################################

# Defning standardization function
def standardize(values):
    return (values - values.mean())/values.std()
  
 # Defning normalization function
def normalize(values):
    if(values.max() == values.min()):
        return (values - values.min())
    else:
        return (values - values.min())/(values.max() - values.min()) 

######################################################################################################################

# Define function to recommend seller
def recommend_sellers(arr_sllrs, arr_cstmrs, sellers):

    return (arr_sllrs - arr_cstmrs)
  
    # Calculate distances between customers and sellers
    dist_matrix = pd.DataFrame(arr_sllrs - arr_cstmrs).apply(normalize).fillna(0).to_numpy()
    dist_vec = 5-5*dist_matrix.sum(axis=1)/dist_matrix.shape[1]
  
    # Unifying sellers Ids and distances
    lines = list(zip(sellers, list(dist_vec)))
  
    return pd.DataFrame(lines, columns=['seller_id', 'score']).sort_values(by='score', ascending=False).head(10)


def main_function(server, database, username, password, customer_id, product_category, payment_installments, payment_boleto, 
                  payment_credit_card, payment_voucher, payment_debit_card, api_key, cep=''):

    # Start a connection using your dredentials
    connection = db_connect(server, database, username, password)

    ## Build customer profile array
    customer_profile = get_customer_profile(connection, customer_id, api_key, payment_installments, payment_boleto, 
                                            payment_credit_card, payment_voucher, payment_debit_card, cep)
    arr_cstmrs = customer_profile[0]
    customer_lat = customer_profile[1]
    customer_lng = customer_profile[2] 
    address = customer_profile[3]

    ## Build sellers profile array
    sellers_profile = get_seller_profile(connection, product_category, customer_lat, customer_lng, api_key)
    arr_sllrs = sellers_profile[0]
    sellers = sellers_profile[1]

    ## Recommend seller
    tb_recommendation = recommend_sellers(arr_sllrs, arr_cstmrs, sellers)

    return {'arr_sllrs':[str(i) for i in tb_recommendation[0].tolist()], 'arr_cstmrs':[str(i) for i in tb_recommendation[1].tolist()]}

    ## Converting response pandas table to json
    tb_recommendation['score'] = tb_recommendation['score'].apply(lambda x:str(x))
    result = tb_recommendation.to_dict('records')

    return {'customer_id':customer_id, 'recommendation':result, 'address':address}