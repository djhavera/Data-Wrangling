
# coding: utf-8

# ### Map Area
# 
# San Antonio, TX United States
# 
# https://mapzen.com/data/metro-extracts/metro/san-antonio_texas/
# 
# This map is of San Antonio, the city where my grandparents lived and where I spent the holidays growing up.

# In[1]:

#!/usr/bin/env python


import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "san-antonio_texas.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 1 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')


# ### Problems Encountered in the Map
# 
# I noticed several problems with the data: 
# 
# Numerous abbreviations for street names ('North US Highway 281','US Highway 281','United States Highway 281')
# 
# 
# Inconsistent postal codes (“78155”, “78155-2214”)
# 
# “Incorrect” postal codes (San Antonio area zip codes all begin with “72” however several zip codes were listed for "282" which corresponds to Charlotte, NC.)

# I queried the street names by using audit.py to identify problems with the street names.  I iterated over each word in an address and corrected them to a mapping list below using an update_name function:
# 
# mapping = { "St": "Street",
#             "St.": "Street",
#             "Ste": "Street",
#             "Rd.": "Road",
#             "Rd" : "Road",
#             "Ave": "Avenue",
#             "Blvd": "Boulevard",
#             "Hwy": "Interstate Highway",
#             "Hiwy": "Interstate Highway",
#             "IH": "Interstate Highway",
#             "I-": "Interstate Highway",
#             "I-H": "Interstate Highway",
#             "Interstate": "Interstate Highway",
#             "Interstate": "Interstate Highway", 
#             "Dr.": "Drive",
#             "Dr": "Drive",
#             "FM": "Farm-to-Market",
#             "Plz": "plaza"
#             }
# 

# ### Data Set
# We can see from the count tags function the amount of nodes, members, tags, and ways below.

# In[3]:

def count_tags(filename):
    tags = {}
    for event, elem in ET.iterparse(filename):
        if elem.tag in tags.keys():
            tags[elem.tag] += 1
        else:
            tags[elem.tag] = 1

    return tags

count_tags(OSM_FILE)


# In[4]:

import pprint
import re
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        k = element.attrib['k']
        if re.search(lower,k):
            keys["lower"] += 1
        elif re.search(lower_colon,k):
            keys["lower_colon"] += 1
        elif re.search(problemchars,k):
            keys["problemchars"] += 1
        else:
            keys["other"] += 1
        return keys
        pass
        
    return keys


def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys


# In[5]:

process_map(OSM_FILE)


# In[6]:

from collections import defaultdict

OSMFILE = "example.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Interstate Highway", "Farm-to-Market" ]

# UPDATE THIS VARIABLE
mapping = { "St": "Street",
            "St.": "Street",
            "Ste": "Street",
            "Rd.": "Road",
            "Rd" : "Road",
            "Ave": "Avenue",
            "Blvd": "Boulevard",
            "Hwy": "Interstate Highway",
            "Hiwy": "Interstate Highway",
            "IH": "Interstate Highway",
            "I-": "Interstate Highway",
            "I-H": "Interstate Highway",
            "Interstate": "Interstate Highway",
            "Interstate": "Interstate Highway", 
            "Dr.": "Drive",
            "Dr": "Drive",
            "FM": "Farm-to-Market",
            "Plz": "plaza"
            }

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)
            

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


# In[7]:

def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


# In[8]:

def update_name(name, mapping):
    m = street_type_re.search(name)
    if m.group() not in expected:
        if m.group() in mapping.keys():
            print "BEFORE"
            print name
            name = re.sub(m.group(), mapping[m.group()], name)
            print "AFTER"
            print name
            
    return name


# In[9]:

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus
import schema

OSM_PATH = "sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


# In[53]:

def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    if element.tag == 'node':
        for attribute in element.attrib:
            if attribute in NODE_FIELDS:
                node_attribs[attribute]=element.attrib[attribute]
        
        sub_iter=element.iter("tag")
        for atr in sub_iter:
            k_val=atr.attrib['k']
            locol=LOWER_COLON.search(k_val)
            prochar=PROBLEMCHARS.search(k_val)
            key_list = k_val.split(':',1)[0]
            k_key=key_list.split(':',1)[0]
            tag_type=key_list[0]
            if locol:
                                    # use cleaning function:
                if k_val == 'addr:street':
                    v_val = update_name(atr.attrib['v'], mapping)
                                     # otherwise:
                else:
                    v_val = atr.attrib['v'] 
            elif prochar:
                v_val=atr.attrib['v']
                continue
            else:
                tag_type="regular"
                v_val=atr.attrib['v']
  
            content={"id":node_attribs['id'],'key':k_key,'value':v_val,'type':tag_type}
            tags.append(content)
        return {'node': node_attribs, 'node_tags': tags}
    
    elif element.tag == 'way':
        for attribute in element.attrib:
            if attribute in WAY_FIELDS:
                way_attribs[attribute]=element.attrib[attribute]
    
        sub_iter=element.iter("nd")
        level=0
        for atr in sub_iter:
            for sub_attrib in atr.attrib:
                if sub_attrib=='ref':
                    content= {"id":way_attribs['id'],'node_id':atr.attrib[sub_attrib],'position':level}
                    way_nodes.append(content)
                    level+=1
        sub_iter=element.iter("tag")
        for atr in sub_iter:
            k_val=atr.attrib['k']
            locol=LOWER_COLON.search(k_val)
            prochar=PROBLEMCHARS.search(k_val)
            key_list = k_val.split(':',1)[0]
            k_key=key_list.split(':',1)[0]
            tag_type=key_list[0]
                       
            if locol:

                    # use cleaning function:
                if k_val == 'addr:street':
                    v_val = update_name(atr.attrib['v'], mapping)
                    # otherwise:
                else:
                    v_val = atr.attrib['v']  
            elif prochar:
                v_val=atr.attrib['v']
                continue
            else:
                tag_type="regular"
                v_val=atr.attrib['v']

            content={"id":way_attribs['id'],'key':k_key,'value':v_val,'type':tag_type}
            tags.append(content)            
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}
# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)



# In[54]:

# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    process_map(OSM_PATH, validate=False)


# ### Uploading Data to SQL Database

# In[12]:

import sqlite3
import csv
from pprint import pprint


# Add nodes_tags table

# In[55]:

sqlite_file = 'sql_db.db'    # name of the sqlite database file

# Connect to the database
conn = sqlite3.connect(sqlite_file)


# In[56]:

# Get a cursor object
cur = conn.cursor()


# In[57]:

cur.execute('''DROP TABLE IF EXISTS nodes_tags''')
conn.commit()


# In[58]:

cur.execute('''
    CREATE TABLE nodes_tags(id INTEGER, key TEXT, value TEXT,type TEXT)
''')
# commit the changes
conn.commit()


# In[59]:

with open('nodes_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]


# In[60]:

# insert the formatted data
cur.executemany("INSERT INTO nodes_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)
# commit the changes
conn.commit()


# Add ways_tags table

# In[61]:

cur.execute('''DROP TABLE IF EXISTS ways_tags''')
conn.commit()


# In[62]:

cur.execute('''
    CREATE TABLE ways_tags(id INTEGER, key TEXT, value TEXT,type TEXT)
''')
# commit the changes
conn.commit()


# In[63]:

with open('ways_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]


# In[64]:

# insert the formatted data
cur.executemany("INSERT INTO ways_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)
# commit the changes
conn.commit()


# add ways_nodes table

# In[65]:

cur.execute('''DROP TABLE IF EXISTS ways_nodes''')
conn.commit()


# In[66]:

cur.execute('''
    CREATE TABLE ways_nodes(id INTEGER, node_id INTEGER, position INTEGER)
''')
# commit the changes
conn.commit()


# In[67]:

with open('ways_nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"), i['position'].decode("utf-8")) for i in dr]


# In[68]:

# insert the formatted data
cur.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db)
# commit the changes
conn.commit()    


# add ways table

# In[69]:

cur.execute('''DROP TABLE IF EXISTS ways''')
conn.commit()


# In[70]:

cur.execute('''
    CREATE TABLE ways(id INTEGER, user TEXT, uid INTEGER, version INTEGER, changeset INTEGER, timestamp TEXT)
''')
# commit the changes
conn.commit()


# In[71]:

with open('ways.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
              i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]


# In[72]:

# insert the formatted data
cur.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db)
# commit the changes
conn.commit()    


# Add nodes table

# In[73]:

cur.execute('''DROP TABLE IF EXISTS nodes''')
conn.commit()


# In[74]:

cur.execute('''
    CREATE TABLE nodes(id INTEGER, lat REAL, lon REAL, user TEXT, uid INTEGER, 
    version INTEGER, changeset INTEGER, timestamp TEXT)
''')
# commit the changes
conn.commit()


# In[75]:

with open('nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"), i['lon'].decode("utf-8"),
              i['user'].decode("utf-8"), i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
              i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]


# In[76]:

# insert the formatted data
cur.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db)
# commit the changes
conn.commit()    


# ### Data Overview

# In[77]:

from pprint import pprint
import os
from hurry.filesize import size

dirpath = os.getcwd()

files_list = []
for path, dirs, files in os.walk(dirpath):
    files_list.extend([(filename, size(os.path.getsize(os.path.join(path, filename)))) 
                       for filename in files])

for filename, size in files_list:
    print '{:.<40s}: {:5s}'.format(filename,size)


# The queries below show that there are 1,244,193 nodes and 144,603 ways in the SQL table.  These amounts tie to count tag functon that I used prior to importing the data set into the SQL table.  This query is a check to help ensure that we have uploaded all the data from the csv file correctly to the SQL database.

# In[78]:

sqlite_file

# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
cur = conn.cursor()

QUERY1 = '''SELECT COUNT(*) 
FROM nodes'''

cur.execute(QUERY1)
result = cur.fetchall()
print(result)


# In[79]:

QUERY2 = '''SELECT COUNT(*) 
FROM ways'''

cur.execute(QUERY2)
result = cur.fetchall()
print(result)


# The number of unique users is 828.

# In[80]:

QUERY3 = '''SELECT COUNT(DISTINCT(e.uid))
FROM (SELECT uid FROM nodes UNION ALL SELECT uid FROM ways) e
'''

cur.execute(QUERY3)
result = cur.fetchall()
print(result)


# The query below shows the number of top 10 amenties in the area.  Places of worship, schools, and restaurants round out the top 3.  a sample query will be something like: select class, count(*) from classes group by 1;
# So, check the query4, you want to group by amenities right!

# In[81]:

results = cur.execute('''SELECT value, COUNT(*) as num FROM nodes_tags WHERE key='amenity' GROUP BY value ORDER BY num DESC LIMIT 10''');
print list(results)


# Interesting that burger restaurants are the most prevalent in San Antonio.  I would have guessed Mexican restaurants.

# In[82]:

QUERY5 = '''SELECT value, COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value='fast_food') i
    ON nodes_tags.id=i.id
WHERE nodes_tags.key='cuisine'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;'''

cur.execute(QUERY5)
result = cur.fetchall()
print(result)


# ### Additional Ideas

# In[83]:

QUERY6 = '''SELECT e.user, COUNT(*) as num
FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e
GROUP BY e.user
ORDER BY num DESC;'''

cur.execute(QUERY6)
result_user = cur.fetchall()
print(result_user)


# In[84]:

import pandas as pd
df = pd.DataFrame(result_user)
df = df.rename(columns={0: 'User', 1: 'Count'})
df.head()


# In[85]:

df.describe()


# There are 828 users in the San Antonio data set.  However, the data seems to be skewed by super users.  For example, the top 4 users all have over 100k contributions each and account for over 1.1M contributions.  I believe these users are skewing the data set and should be removed from any further data analysis.

# In[86]:

df2 = df


# In[87]:

df2.head()


# In[88]:

df2 = df2.drop(df.index[[0,1,2,3]])


# In[89]:

df2.head()


# In[90]:

df2.describe()


# The mean drops significantly from 1677 to 349 contributions per user just by removing the top four users from the data set.  However, there still appears to be a major drop off in user contributions with 50% of the users only making 2 contributions.  Additionally, the 75th percentile is at 74 contributions per user. Motivating the 50th and 75th percentile user base should be a next step to continually improve the accuracy of the San Antonio area.
# 
# One possible way to increase involvement is to use gamification.  For example, Treehouse is a virtual training academy for learning code, app development, and business skills, used by beginners to learn valuable career skills and experienced professionals for career advancement.  Students earn badges and points as they progress thru courses which impresses potential employers.  Openstreetmap.org should team up with virtual training academies like Treehouse and integrate map updates as part of data wrangling curriculm.  Since Treehouse is free there should be a lot of opportunity for participation from new users.
# 
# 
# Benefits
#     1) Many available students to update map accuracy.
#     2) Utilizes successful gamification system already used at Treehouse
# 
# Anticipated Issues
#     1) How long will this be sustainable?  How much work is actually needed to clean up the maps?  Is this a 1 year effort or 10?
#     2) Version control - How do you prevent overwriting of contributions by others.
