#---------------------------------------------------------------------------------------------------------
# Yelp API - 8/2018
# Pull Health Business Entries within 25 mi radius of each county center and save (will need deduped later)
#---------------------------------------------------------------------------------------------------------

#------------------------------------------------------------
# Import Packages
#------------------------------------------------------------
from yelpapi import YelpAPI
from pprint import pprint
import pandas as pd
import time
import config
import sys
import gzip
import json

#------------------------------------------------------------
# Paths and Dependencies
#------------------------------------------------------------

# File paths
path_counties = '/home/kmcampbell/Desktop/yelp/geos/counties_clean.csv'

date = pd.to_datetime('now').strftime("%m-%d-%Y_%H%M%S")
filename = 'healthyelp_{}.json.gz'.format(date)
outfile = gzip.open('/home/kmcampbell/Desktop/yelp/data/{}'.format(filename), 'wb')
processlog = '/home/kmcampbell/Desktop/yelp/data/process_log.csv'

#Set radius in meters
radius = 40000

def countyparse(row):
    latitude = row['Latitude']
    longitude = row['Longitude']
    county = row['County'].lower()
    fips = str(row['FIPS'])
    if len(fips) == 4:
        fips = "0" + fips
    return (latitude, longitude, county, fips)

#------------------------------------------------------------
# Access API, Pull Businesses
#------------------------------------------------------------

# Connect
yelp_api = YelpAPI(config.apikey)

# Setup Geos to Iterate
df_geos = pd.read_csv(path_counties)

# Read in Process Log to find current position
process_log = pd.read_csv(processlog)
if len(process_log) == 0:
    currfips = '01001'
else:
    last = process_log.tail(1)
    currfips = str(last['endFIPS'].values[0])
    if len(currfips) == 4:
        currfips = "0" + currfips


# Pull Data Till Dies
print "Starting Collection:"
print "Current FIPS: " + currfips
callcnt = 0
geocnt = 0
go = 0
totalbusinesses = []
for index, row in df_geos.iterrows():
    # Parse Row
    latitude, longitude, county, fips = countyparse(row)
    # loop through until get to the fips we want to start processing from
    if currfips == fips:
        go = 1
    if go == 1:
        print str(geocnt) + ": " + fips + ", " + county + ", " + str(latitude) + ", " + str(longitude)
        geocnt += 1

        # Write out and clear store periodically just in case
        if geocnt % 20 == 0:
            print "--------------------------------------"
            print "WRITING OUT!"
            print "--------------------------------------"
            # Write out current store
            for bus in totalbusinesses:
                outfile.write(json.dumps(bus).encode('ascii',errors='ignore'))
                outfile.write('\n'.encode('ascii'))
            # clear store list
            totalbusinesses = []

        # Get first 50 and total count
        try:
            print "getting first 50"
            response = yelp_api.search_query(categories='health', latitude=latitude, longitude=longitude, radius=radius, limit=50, offset=0)
            time.sleep(1)
            callcnt += 1
            allbusinesses = response['businesses']
            total = response['total']
            print "theres gonna be " + str(total)
        except Exception as e:
            print(e)
            print "BROKE AT: " + str(geocnt) + ": " + fips + ", " + county + ", " + str(latitude) + ", " + str(longitude)
            # Write out current store
            print "--------------------------------------"
            print "WRITING OUT " + str(len(totalbusinesses)) + " BUSINESSES"
            print "--------------------------------------"
            for bus in totalbusinesses:
                outfile.write(json.dumps(bus).encode('ascii',errors='ignore'))
                outfile.write('\n'.encode('ascii'))
            outfile.close()
            # Write out where we left off in process log(will re-pull current one that broke)
            process_log = process_log.append({'File': filename, 'startFIPS': currfips, 'endFIPS': fips}, ignore_index=True)
            process_log.to_csv(processlog, index=False)
            # Break program (have to manually restart)
            sys.exit(1)

        # Loop and get the rest of the businesses in batches of 50
        if total > 50:
            if total > 1000:
                total = 1000
            for i in xrange(50, total, 50):
                try:
                    print "getting the rest"
                    response = yelp_api.search_query(categories='health', latitude=latitude, longitude=longitude, radius=radius, limit=50, offset=i)
                    time.sleep(1)
                    callcnt += 1
                    businesses = response['businesses']
                    allbusinesses.extend(businesses)
                    print fips + " --> " + str(len(allbusinesses))
                except Exception as e:
                    print(e)
                    print "BROKE AT: " + str(geocnt) + ": " + fips + ", " + county + ", " + str(latitude) + ", " + str(longitude)
                    # Write out current store
                    print "--------------------------------------"
                    print "WRITING OUT " + str(len(totalbusinesses)) + " BUSINESSES"
                    print "--------------------------------------"
                    for bus in totalbusinesses:
                        outfile.write(json.dumps(bus).encode('ascii',errors='ignore'))
                        outfile.write('\n'.encode('ascii'))
                    outfile.close()
                    # Write out where we left off in process log(will re-pull current one that broke)
                    process_log = process_log.append({'File': filename, 'startFIPS': currfips, 'endFIPS': fips}, ignore_index=True)
                    process_log.to_csv(processlog, index=False)
                    # Break program (have to manually restart)
                    sys.exit(1)
            # Add to the writeout list
            totalbusinesses.extend(allbusinesses)
            print "Saved businesses for " + fips
            print
        else:
            #Add to the writeout list
            totalbusinesses.extend(allbusinesses)
            print "Saved businesses for " + fips
            print

# Write out current store
print "--------------------------------------"
print "FINAL WRITE OUT: " + str(len(totalbusinesses)) + " BUSINESSES"
print "--------------------------------------"
for bus in totalbusinesses:
    outfile.write(json.dumps(bus).encode('ascii',errors='ignore'))
    outfile.write('\n'.encode('ascii'))
outfile.close()
