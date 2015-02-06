# City Bike Exercise. Unit 3: leson 1
import requests # package that handles going to Internet and downloading the resulting page
# requests documentaiton at: http://docs.python-requests.org/en/latest/

# 1. Pass the URL for Citi BIke station. Puts file into python for manipulation.

r = requests.get('http://www.citibikenyc.com/stations/json')
r.text #gives string of data
r.json() #gives data in JSON format - data can be accessed by their keys:
r.json().keys()
#[u'executionTime', u'stationBeanList']
r.json()['executionTime']  #gives string with time the file was created
r.json()['stationBeanList']  #gives lsit of stations
len(r.json()['stationBeanList']) #to get number of docks: 332
r.json()['stationBeanList'] [0]  #gets first station and its keys/values

# 2. test that you have all the fields (so that you can set up a database!) 
# (gets list of keys for each station)
#    run the data through a loop to gather all the fields:
key_list =[] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

# 3. Getting Data into DataFrame
# to import data to panda use:
from pandas.io.json import json_normalize
df = json_normalize(r.json()[‘stationBeanList’])

# 4. Check range of values
import matplotlib.pyplot as plt
import pandas as pd
df['availableBikes'].hist()
plt.savefig('hist_availableBikes.png')
plt.show()
df['totalDocks'].hist()
plt.savefig('hist_totalDocks.png')
plt.show()

df.shape   #shape of df
df.statusValue.unique()  # array([u'In Service', u'Not In Service'], dtype=object)
len(df[df.statusValue=='Not In Service'])  #stations not in service (4)
len(df[df.statusValue=='In Service'])  #stations not in service (4)
len(df[df.testStation==True])  #are there any test stations? NO
df_bikes = pd.DataFrame([df.id,df.availableBikes/df.totalDocks]).T
df_bikes.columns = ['id', 'bikesPerDock']
df_bikes.set_index['id']
df_bikes.mean()
df_bikes.median()
df_bikes[df.statusValue=='In Service'].mean()  #mean for bikes in service
df_bikes[df.statusValue=='In Service'].median()  #median for bikes in service
# or 
condition = (df['statusValue'] == 'In Service')
df[condition]['totalDocks'].mean()

# 5. Create SQLite database.
# 5a. create data table to store data:

import sqlite3 as lite
con = lite.connect('citi_bike.db')
cur = con.cursor()

# use 'with' keyword as contect manager. At end of codeblock the transaction will be saved/committed to the db.
#  (equivalent to con.commit(), but cleaner/more readable.)
with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

#    cur.exectue('CREATE TABLE citibike_reference (
#    		id INT PRIMARY KEY, 
#    		totalDocks INT, 
#    		city TEXT, 
#    		altitude INT, 
#    		stAddress2 TEXT, 
#    		longitude NUMERIC, 
#    		postalCode TEXT, 
#    		testStation TEXT, 
#    		stAddress1 TEXT, 
#    		stationName TEXT, 
#    		landMark TEXT, 
#    		latitude NUMERIC, 
#    		location TEXT )'
#    	)

# 5b. populate table with values
#       a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

# the for loop to populate values in the database
# using a type of SQL query called a "parameterized query" with '?' standing in for values which are refenced in cur.execute.
#     - the list of values has to match up!
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

# to get multiple readings by minute, the availabeBikes table is going to need to be different.
#	- station ID 'id' is going to b the column name, but since a column name can't start iwth a numbr, need to put a character in front of the number:
#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
 # instead of 333 column names being written out, usr Python to handle it all. 
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

 # Populate table with values for available bikes.
 # a package with datetime objects
import time

# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 

import collections

#take the string and parse it into a Python datetime object
#  (Python has a datetime object that stores datetime values. 
#       - this object has attributes for year, month, day , hour..microsecs, and timezone.
#        - below is a naive datetime object since no timezone is specified.)
exec_time = parse(r.json()['executionTime'])

#create an entry for execution time by inserting it into the database:
# (strftime() takes a datetime object and creates a string representing time under the control of an explicity format string.)
#  strftime() formats the time (altrnate function is striptime()), which is used to parse a string into proper time format.
#  see: https://docs.python.org/2/library/datetime.html#module-datetime
# %s formats time into Unix time (Epoch time), which is the number of seconds since 1 Jan 1970 00:00:00 UTC
#  Each platform has its own system time.
with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))

# iterate through stations in 'stationBeanList':
id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station
#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")

# Data is now loaded and the script can be automated to run every minute to download the data, proces sresult and upload it to the database!

######################################
r.status_code
r.headers['content-type']
r.encoding
r.text
r.json()
