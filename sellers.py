import requests


######################################### CREATING FUNCTION: GET_GEOLOCATION #########################################
######################################################################################################################

def get_geolocation(query, api_key):

    # base url
    base_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?"

    # building url
    parameters = "input={query}&inputtype=textquery&fields=formatted_address%2Cgeometry&key={api_key}"
    url = base_url + parameters.format(query=query, api_key=api_key)

    # define paylodas and headers as empty
    payload={}
    headers = {}

    # make get request and convert response to json
    data = requests.request("GET", url, headers=headers, data=payload).json()

  
    # Parse information
    try:
        address = data['candidates'][0]['formatted_address'] 
    except:
        return (None, None, None)
  
    # Parse information | Latitude
    try:
        latitude = data['candidates'][0]['geometry']['location']['lat']
    except:
        return (None, None, None)
  
    # Parse information | Longitude
    try:
        longitude = data['candidates'][0]['geometry']['location']['lng']
    except:
        return (None, None, None)
  
    return (address, latitude, longitude)