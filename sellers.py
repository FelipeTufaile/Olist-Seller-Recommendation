import requests


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