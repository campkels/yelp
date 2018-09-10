#------------------------------------------------------------
# Import Packages
#------------------------------------------------------------

import json
import shapefile #pip install pyshp
from rtree import index
from shapely.geometry import shape, Point, Polygon
import statistics
import time
import os
import csv
import random

# Function to Lookup County
#------------------------------------------------------------

def latlong_to_county(lat, long):

	#transform lat long to shapely object ***lat long is switched!!***
	point = Point(long, lat)

	#check only within bounding boxes that contains point
	for j in idx.intersection(point.coords[0]):
		#check actual point in polygon
		if point.within(polygons[j]):
			Fips = polygons_sf.record(j)[3]
			County_Name = polygons_sf.record(j)[4]
			return Fips, County_Name
			break

# Function to Parse Yelp Record
#------------------------------------------------------------

def parse_yelp(record):
	jrec = json.loads(record)
	alias= jrec['alias'].encode('utf-8')
	cats = []
	for entry in jrec['categories']:
		cats.append(entry['alias'])
	generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals = dummy(cats)
	lat = jrec['coordinates']['latitude']
	lon = jrec['coordinates']['longitude']
	address = ""
	for line in jrec['location']['display_address']:
		address += " " + line.encode('utf-8')
	zipcode = jrec['location']['zip_code']
	state = jrec['location']['state']
	name = jrec['name'].encode('utf-8')
	price = jrec['price']
	rating = jrec['rating']
	review_count = jrec['review_count']
	return alias, name, cats, generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals, price, rating, review_count, address, zipcode, state, lat, lon

def dummy(catlist):
	generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals = (0,0,0,0,0,0)
	for c in catlist:
		if c == 'generaldentistry':
			generaldentistry = 1
		elif c == 'physicians':
			physicians = 1
		elif c == 'c_and_mh':
			c_and_mh = 1
		elif c == 'urgent_care':
			urgent_care = 1
		elif c == 'pharmacy':
			pharmacy = 1
		elif c == 'hospitals':
			hospitals = 1
	return generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals

#------------------------------------------------------------
# Create Spacial Index from County Shapefile
#------------------------------------------------------------

shape_file = '/home/kmcampbell/Desktop/yelp/geos/tl_2015_us_county/tl_2015_us_county.shp'

#convert shapefile to shapely polygon objects
polygons_sf = shapefile.Reader(shape_file)
polygons_shapes = polygons_sf.shapes()
polygons_points = [q.points for q in polygons_shapes]
polygons = [Polygon(q) for q in polygons_points]

#build spatial index of bounding boxes of polygons
idx = index.Index()
count = -1
for q in polygons_shapes:
	count += 1
	idx.insert(count, q.bbox)


#------------------------------------------------------------
# Read in File, Parse, Assign County, Write out
#------------------------------------------------------------

file = '/home/kmcampbell/Desktop/yelp/data/deduped.json'

out = open( '/home/kmcampbell/Desktop/yelp/data/enhanced.csv', 'wb' )
writer = csv.writer( out )
header = ['alias', 'name', 'categories', 'generaldentistry', 'physicians', 'c_and_mh', 'urgent_care', 'pharmacy', 'hospitals', 'price', 'rating', 'review_count', 'address', 'zipcode', 'state', 'lat', 'lon', 'FIPS', 'county']
writer.writerow(header)

allcats = {}
cnt = 0
stream = open('/home/kmcampbell/Desktop/yelp/data/deduped.json')
for line in stream:
	cnt += 1
	print cnt
	alias, name, cats, generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals, price, rating, review_count, address, zipcode, state, lat, lon = parse_yelp(line)
	# Keep track of category counts
	for c in cats:
		if c in allcats.keys():
			#add one
			allcats[c] += 1
		else:
			#add category
			allcats[c] = 1
	#allcats.extend(cats)
	#allcats = list(set(allcats))
	categories = ", ".join(cats)
	try:
		FIPS, county = latlong_to_county(lat,lon)
	except:
		FIPS = None
		county = None
	row = [alias, name, categories, generaldentistry, physicians, c_and_mh, urgent_care, pharmacy, hospitals, price, rating, review_count, address, zipcode, state, lat, lon, FIPS, county]
	writer.writerow(row)

#Print counts, highest first
allcats_view = [ (v,k) for k,v in allcats.iteritems() ]
allcats_view.sort(reverse=True) # natively sort tuples by first element
for v,k in allcats_view:
    print "%s: %d" % (k,v)

out.close()
