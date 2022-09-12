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
        data = np.ones(tb_customer_profile.shape[1]-1).tolist()
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
        tb_customer_profile[tb_customer_profile.columns[1:13]] = tb_customer_profile[tb_customer_profile.columns[1:13]].apply(lambda x:-float(x))

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
    
    return (tb_sellers_profile[tb_sellers_profile.columns[1:]].to_numpy(), list(tb_sellers_profile['seller_id']), list(columns)[1:-2])


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

# Get variables that have the most positive and negative contributions
def get_vip(values, features_averages, address):

    if(address==''):

        # Calculating score per category
        size = (values[0] + values[5] + values[6])/(features_averages[0] + features_averages[5] + features_averages[6])
        review = (values[1] + values[2])/(features_averages[1] + features_averages[2])
        description = (values[3] + values[4])/(features_averages[3] + features_averages[4])
        installment = values[7]/features_averages[7]
        payment_method = sum(values[8:12])/sum(features_averages[8:12])
        freight = (values[12] + values[15])/(features_averages[12] + features_averages[13])
        distance = (values[13] + values[14])/(features_averages[13] + features_averages[14])
        delay = (values[16] + values[17])/(features_averages[16] + features_averages[17])

        categories = ['size', 'review', 'description', 'installment', 'payment_method', 'freight', 'distance', 'delay']
        category_score = [size, review, description, installment, payment_method, freight, distance, delay]
        
        lowest_score_feature = [categories[category_score.index(min(category_score))], min(category_score)]
        highest_score_feature = [categories[category_score.index(max(category_score))], max(category_score)]
        
        return {'lowest_score_feature':lowest_score_feature, 'highest_score_feature':highest_score_feature}
    
    else:
        # Calculating score per category
        installment = values[7]/features_averages[7]
        payment_method = sum(values[8:12])/sum(features_averages[8:12])
        freight = (values[12] + values[15])/(features_averages[12] + features_averages[13])
        distance = (values[13] + values[14])/(features_averages[13] + features_averages[14])

        categories = ['installment', 'payment_method', 'freight', 'distance']
        category_score = [installment, payment_method, freight, distance]
        
        lowest_score_feature = [categories[category_score.index(min(category_score))], min(category_score)]
        highest_score_feature = [categories[category_score.index(max(category_score))], max(category_score)]
        
        return {'lowest_score_feature':lowest_score_feature, 'highest_score_feature':highest_score_feature}

def get_priorities(delivery, review, freight):

    vector = np.zeros(18).tolist()
    if (delivery==1):
        vector[13] = 1
        vector[14] = 1
    if (review==1):
        vector[1] = 1
        vector[2]
    if (freight==1):
        vector[12] = 1
        vector[15] = 1
    
    return vector
    

# Define function to recommend seller
def recommend_sellers(arr_sllrs, arr_cstmrs, sellers, address, priority_delivery, priority_review, priority_freight):

    # Converting profile lists to numpy arrays
    arr_sllrs = np.array(arr_sllrs)
    arr_cstmrs = np.array(arr_cstmrs)
    
    # Calculating the difference between customer profile and seller profiles
    difference_matrix = arr_sllrs.astype(np.float32) - arr_cstmrs.astype(np.float32)

    # Calculate distances between customers and sellers
    dist_matrix = pd.DataFrame(difference_matrix).apply(normalize).fillna(0).to_numpy()
    dist_matrix = 10*(np.array([1]).astype(np.float32) - dist_matrix)

    # Check if user has given any priority to a specific set of features
    if ((int(priority_delivery) > int(0)) | (int(priority_review) > int(0)) | (int(priority_freight) > int(0))):
        priority_vector = get_priorities(priority_delivery, priority_review, priority_freight)
        dist_matrix = dist_matrix*np.array(priority_vector)

    # Calculating averages
    features_averages = np.mean(dist_matrix, axis=0)

    col_hscore, val_hscore, col_lscore, val_lscore = [], [], [], []
    # Calculate variable importance
    for seller_profile_distance in dist_matrix:
        col_hscore.append(get_vip(seller_profile_distance.tolist(), features_averages.tolist(), address)['highest_score_feature'][0])
        val_hscore.append(get_vip(seller_profile_distance.tolist(), features_averages.tolist(), address)['highest_score_feature'][1])
        col_lscore.append(get_vip(seller_profile_distance.tolist(), features_averages.tolist(), address)['lowest_score_feature'][0])
        val_lscore.append(get_vip(seller_profile_distance.tolist(), features_averages.tolist(), address)['lowest_score_feature'][1])

    # Calculating distance vector and converting it between scores 0 to 5
    dist_vec = 5*dist_matrix.sum(axis=1)/180
  
    # Unifying sellers Ids and distances
    lines = list(zip(sellers, list(dist_vec), col_hscore, val_hscore, col_lscore, val_lscore))

    # Creating final result table
    result_columns=['seller_id', 'score', 'highest_score_feature', 'highest_score', 'lowest_score_feature', 'lowest_score']

    result_table = pd.DataFrame(lines, columns=result_columns)
  
    return result_table.sort_values(by='score', ascending=False).head(10)


def main_function(server, database, username, password, customer_id, product_category, payment_installments, payment_boleto, 
                  payment_credit_card, payment_voucher, payment_debit_card, api_key, priority_delivery, priority_review, 
                  priority_freight, cep=''):

    # Start a connection using your dredentials
    connection = db_connect(server, database, username, password)

    ## Build customer profile array
    customer_profile = get_customer_profile(connection, customer_id, api_key, payment_installments, payment_boleto, 
                                            payment_credit_card, payment_voucher, payment_debit_card, cep)
    
    arr_cstmrs = customer_profile[0].tolist()
    customer_lat = customer_profile[1]
    customer_lng = customer_profile[2] 
    address = customer_profile[3]

    ## Build sellers profile array
    sellers_profile = get_seller_profile(connection, product_category, customer_lat, customer_lng, api_key)

    arr_sllrs = sellers_profile[0].tolist()
    sellers = sellers_profile[1]
    columns = sellers_profile[2]

    ## Recommend seller
    tb_recommendation = recommend_sellers(arr_sllrs, arr_cstmrs, sellers, address, priority_delivery, priority_review, priority_freight)

    ## Converting response pandas table to json
    tb_recommendation['score'] = tb_recommendation['score'].apply(lambda x:str(x))
    tb_recommendation['highest_score'] = tb_recommendation['highest_score'].apply(lambda x:str(x))
    tb_recommendation['lowest_score'] = tb_recommendation['lowest_score'].apply(lambda x:str(x))
    result = tb_recommendation.to_dict('records')

    return {'customer_id':customer_id, 'recommendation':result, 'address':address}