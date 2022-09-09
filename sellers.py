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
    driver = '{ODBC Driver 17 for SQL Server}'
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
def get_customer_profile(server, database, username, password, customer_id, api_key, cep=''):

  # Start a connection using your dredentials
    database = db_connect(server, database, username, password)

    # Building query
    query = "SELECT * FROM dbo.tb_customers_profile WHERE customer_unique_id = '{customer_id}'".format(customer_id=customer_id)

    # Get columns
    columns = database.execute(query).keys()

    # Get data
    data = database.execute(query)

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

        profile = np.zeros(tb_customer_profile.shape[1]-1).tolist()
    
        # Define an empty vector for new customer
        #return (np.zeros(tb_customer_profile.shape[1]-1), True, customer_lat, customer_lng, address)
        return {'customer_profile':profile, 
                'new_customer': True, 
                'location_latitude':customer_lat, 
                'location_longitude':customer_lng,
                'location_address':'No address'}
   
    else:
    
        customer_lat = tb_customer_profile['customer_lat'][0]
        customer_lng = tb_customer_profile['customer_lng'][0]
    
        # Removing spare columns
        tb_customer_profile = tb_customer_profile.drop(columns=['customer_lat', 'customer_lng'], axis=0)
    
        ## Adjusting values that are inversely proportional in customer table
        tb_customer_profile[tb_customer_profile.columns[1:13]] = tb_customer_profile[tb_customer_profile.columns[1:13]].apply(lambda x:-x)

        profile = tb_customer_profile[tb_customer_profile.columns[1:]].to_numpy().tolist()
        #return (tb_customer_profile[tb_customer_profile.columns[1:]].to_numpy(), False, customer_lat, customer_lng, 'No Address')
        return {'customer_profile':profile, 
                'new_customer': False, 
                'location_latitude':customer_lat, 
                'location_longitude':customer_lng,
                'location_address':'No address'}