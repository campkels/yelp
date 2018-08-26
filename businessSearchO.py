#---------------------------------------------------------------------------------------------------------
# Yelp API - 8/2018
# Connects to the Yelp API and pulls counts by city
#---------------------------------------------------------------------------------------------------------

#------------------------------------------------------------
# Import Packages
#------------------------------------------------------------
from yelpapi import YelpAPI
from pprint import pprint
import pandas as pd
import time

#------------------------------------------------------------
# Paths and Dependencies
#------------------------------------------------------------
# Path for File with City Names and Coordinates 
path_cities = "../cities_coords.csv"
# Path for Out File
out_cities = "../counts10mi.csv"

# API account (get yours at https://www.yelp.com/developers/documentation/v3/get_started)
apikey = ""

# Set Radius
radius = 16094

#------------------------------------------------------------
# Access API, Pull Counts
#------------------------------------------------------------

# Connect
yelp_api = YelpAPI(apikey)

# Setup City Geos to Iterate
df_cities = pd.read_csv(path_cities)
#[0:25]

# Iterate and Pull Counts - Lat/Long + Radius
cnt = 0
for index, row in df_cities.iterrows():
    cnt += 1
    # Query location variables
    latitude = row['latitude']
    longitude = row['longitude']
    city = row['city'].lower()
    state = row['state'].lower()
    print( str(cnt) + ": " + city + ", " + state + ", " + str(latitude) + ", " + str(longitude) )
    # Count for GB's
    response = yelp_api.search_query(categories='gaybars', latitude=latitude, longitude=longitude, radius=radius)
    # latitude = response['region']['center']['latitude']
    # longitude = response['region']['center']['longitude']
    count = response['total']
    # df_cities.set_value(index,'latitude', latitude)
    # df_cities.set_value(index,'longitude', longitude)
    df_cities.set_value(index,'n_gaybars', count)
    # Count for GN Baths
    response = yelp_api.search_query(attributes='gender_neutral_restrooms', latitude=latitude, longitude=longitude, radius=radius)
    count = response['total']
    df_cities.set_value(index,'n_gnbath', count)
    # Count for Tot Bars
    response = yelp_api.search_query(categories='bars', latitude=latitude, longitude=longitude, radius=radius)
    count = response['total']
    df_cities.set_value(index,'n_totbars', count)
    # Count for Tot Businesses
    response = yelp_api.search_query(latitude=latitude, longitude=longitude, radius=radius)
    count = response['total']
    df_cities.set_value(index,'n_totbus', count)
    time.sleep(1)
    # Write out periodically cause it likes to die for no reason
    if cnt % 10 == 0:
        df_cities.to_csv(out_cities)
        print("wrote out")
        time.sleep(5)

# Calculate Proportions
df_cities['prop_gaybars'] = df_cities['n_gaybars']/df_cities['n_totbars']
df_cities['prop_gnbath'] = df_cities['n_gnbath']/df_cities['n_totbus']

# Save Complete File
df_cities.to_csv(out_cities)

# Original Run by City Name to Get Coordinates
# cnt = 0
# for index, row in df_cities.iterrows():
#     cnt += 1
#     # Query location variables
#     city = row['city'].lower()
#     state = row['state'].lower()
#     print str(cnt) + ": " + city + ", " + state
#     # Count for GB's
#     response = yelp_api.search_query(categories='gaybars', location='{}, {}'.format(city, state), limit=5)
#     latitude = response['region']['center']['latitude']
#     longitude = response['region']['center']['longitude']
#     count = response['total']
#     df_cities.set_value(index,'latitude', latitude)
#     df_cities.set_value(index,'longitude', longitude)
#     df_cities.set_value(index,'n_gaybars', count)
#     # Count for GN Baths
#     response = yelp_api.search_query(attributes='gender_neutral_restrooms', location='{}, {}'.format(city, state), limit=5)
#     count = response['total']
#     df_cities.set_value(index,'n_gnbath', count)
#     # Count for Tot Bars
#     response = yelp_api.search_query(categories='bars', location='{}, {}'.format(city, state), limit=5)
#     count = response['total']
#     df_cities.set_value(index,'n_totbars', count)
#     # Count for Tot Businesses
#     response = yelp_api.search_query(location='{}, {}'.format(city, state), limit=5)
#     count = response['total']
#     df_cities.set_value(index,'n_totbus', count)
#     time.sleep(1)