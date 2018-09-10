#---------------------------------------------------------------------------------------------------------
# Yelp API - 8/2018
# Read in data pulled from Yelp API, deduplicate, and restructure
#---------------------------------------------------------------------------------------------------------

#------------------------------------------------------------
# Import Packages
#------------------------------------------------------------

from pprint import pprint
import pandas as pd
import time
import sys
import gzip
import json
import glob
import os

#------------------------------------------------------------
# Combine and Restructure Files
#------------------------------------------------------------

# Combine Files
#---------------------------
path = "/home/kmcampbell/Desktop/yelp/data/"
allFiles = glob.glob(os.path.join(path, "*.json.gz"))
df = pd.concat((pd.read_json(gzip.open(f, 'rb'), lines=True) for f in allFiles))

#print df.head()
print df.shape
print df.describe()

# Check Counts
#---------------------------
cnt = 0
for i in sorted(allFiles):
        #print i
        stream = gzip.open(i)
        for line in stream:
            cnt += 1

print "CHECK: " +  str(cnt)

# Dedup
#---------------------------
deduped = df.drop_duplicates(['alias'])

#print df.head()
print "DEDUPED!"
print deduped.shape
print deduped.describe()
print deduped.columns

# Subset to records with Price
#---------------------------
full = deduped.dropna(subset=['price', 'rating'])

print "FULL!"
print full.head()
print full.size
print full.shape
print full.describe()
print full.columns

# Save New JSONs
#---------------------------
deduped.to_json("/home/kmcampbell/Desktop/yelp/data/deduped.json", orient='records', lines=True)
full.to_json("/home/kmcampbell/Desktop/yelp/data/complete.json", orient='records', lines=True)
#Are escaped slashes in the URL an issue? Could use json library instead, I guess..

# Get List of coordinates, curious how the complete records are distributed
cnt = 0
coords = open('/home/kmcampbell/Desktop/yelp/data/coords_all.txt', "w")
stream = open('/home/kmcampbell/Desktop/yelp/data/deduped.json')
for line in stream:
    cnt += 1
    jrec = json.loads(line)
    lat = jrec['coordinates']['latitude']
    lon = jrec['coordinates']['longitude']
    coords.write("{}, {}\n".format(lat, lon))
coords.close()

# Pull out coordinates in dataframe
# Assign to county
# plot average ratings by county (could later stratify by type of )
