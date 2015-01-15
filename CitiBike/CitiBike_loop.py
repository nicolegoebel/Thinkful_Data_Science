# -*- coding: utf-8 -*-

#define source code encoding, add this to the top of your script
#    it works differently in console and in the IDE is, likely, because of different default encodings set. 
#     You can check it by running:
#     import sys
#     print sys.getdefaultencoding()
import sys
print sys.getdefaultencoding()

# City Bike Exercise. Unit 3: leson 1
import requests # package that handles going to Internet and downloading the resulting page
# requests documentaiton at: http://docs.python-requests.org/en/latest/
import matplotlib.pyplot as plt
import pandas as pd
from pandas.io.json import json_normalize # to import data to panda use:
import sqlite3 as lite
import time
from dateutil.parser import parse  # a package for parsing a string into a Python datetime object
import collections
import pdb
from datetime import datetime


# Create tables
con = lite.connect('citi_bike.db')
cur = con.cursor()
#   drop tables if they already exist
cur.execute("DROP TABLE IF EXISTS citibike_reference")# insert data into the two tables
cur.execute("DROP TABLE IF EXISTS available_bikes")

m=0
while m<60:
	# 1. Pass the URL for Citi BIke station. Puts file into python for manipulation.
	r = requests.get('http://www.citibikenyc.com/stations/json')
	# 2. test that you have all the fields (so that you can set up a database!) 
	# (gets list of keys for each station)
	#    run the data through a loop to gather all the fields:
	#key_list =[] #unique list of keys for each station listing
	#for station in r.json()['stationBeanList']:
	#	for k in station.keys():
	#		if k not in key_list:
	#			key_list.append(k)
	# 3. Getting Data into DataFrame
	df = json_normalize(r.json()["stationBeanList"])
	# 5. Create SQLite database.
	# 5a. create data table to store data:
	# use 'with' keyword as contect manager. At end of codeblock the transaction will be saved/committed to the db.
	#  (equivalent to con.commit(), but cleaner/more readable.)
	if m==0:
		with con:
			cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
	#cur = con.cursor()
	## use 'with' keyword as contect manager. At end of codeblock the transaction will be saved/committed to the db.
	##  (equivalent to con.commit(), but cleaner/more readable.)
	#with con:
	#    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
	## 5b. populate table with values
	##       a prepared SQL statement we're going to execute over and over again
	sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
	# the for loop to populate values in the database
	# using a type of SQL query called a "parameterized query" with '?' standing in for values which are refenced in cur.execute.
	#     - the list of values has to match up!	with con:
	if m==0:
		with con:
			for station in r.json()['stationBeanList']:
				#id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
				cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))	
	# to get multiple readings by minute, the availabeBikes table is going to need to be different.
	#	- station ID 'id' is going to b the column name, but since a column name can't start iwth a numbr, 
	#           need to put a character in front of the number
	#extract the column from the DataFrame and put them into a list
	station_ids = df['id'].tolist()
	#add the '_' to the station name and also add the data type for SQLlite
	station_ids = ['_' + str(x) + ' INT' for x in station_ids]
	
	#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT') added
	# instead of 333 column names being written out, use Python to handle it all
	if m==0:
		with con:
			cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")
	# Populate table with values for available bikes.
	#take the string and parse it into a Python datetime object
	#        - below is a naive datetime object since no timezone is specified.
	exec_time = parse(r.json()["executionTime"])
	etime=r.json()["executionTime"]
	#create an entry for execution time by inserting it into the database
	# (strftime() takes a datetime object and creates a string representing time under the control of an explicity format string.
	#  strftime() formats the time (altrnate function is striptime()), which is used to parse a string into proper time format
	#  see: https://docs.python.org/2/library/datetime.html#module-datetim
	# %s formats time into Unix time (Epoch time), which is the number of seconds since 1 Jan 1970 00:00:00 UT
	#  Each platform has its own system time
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
    	# Data is now loaded and the script can be automated to run every minute to download the data, process result and upload it to the database!
    	time.sleep(61)  #code sleeps for 61 seconds since want to record bikes per minute see: https://docs.python.org/2/library/time.html
	m=m+1
	print m
pdb.set_trace()
# 3.   join data together
#cur.execute("SELECT * FROM available_bikes")
#cur.execute("SELECT name,  state, year, warm_month, cold_month, average_high  FROM cities JOIN weather ON name=city");
#       get rows of data (fetchall)
#rows = cur.fetchall()
#        get column names
#cols = [desc[0] for desc in cur.description]
#  4.    load data into pandas DataFrame
#df = pd.DataFrame(rows, columns=cols)
#    close connection
#con.close()
# Finally, print the result.
#  iterate over each row in the DataFrame with iterrows(). Note that this function returns both the index and the row. 
#for index, row in df.iterrows():
#	print "%s, %s is warmest in %s" % (row['name'], row['state'], row['warm_month'])
#	#print index
#
#load data into a pandas DF
#1. set up access to SQLite database
con = lite.connect('citi_bike.db')
cur = con.cursor()
#read in data
df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')
#save as csv
df.to_csv("citibikeDB_{0}.csv".format(etime[0:10]), sep=',') 
if locals().has_key("df")==False:
    df = pd.read_csv('citibikeDB_2015-01-13.csv')
#Design an algorithm
# collect the difference between two elements and keep a running total of that difference over all the elements.
# having the values sorted in time in order to make the difference from one minute
# process each column and calculate the change each minute:
# enumerate() function returns not only the item in the list but also the index of the item. 
#     - this allows us to find the value (with index of k) just after it in sequence (k + 1). 
# We run the loop until k is equal to the index for the second to last element in the list.
hour_change = collections.defaultdict(int)
for col in df.columns[1:]:
    station_vals = df[col].tolist()
    station_id = col[1:] #trim the "_"
    station_change = 0
    for k,v in enumerate(station_vals):
    	print "k = {0}, v = {1}".format(k, v)
    	if k < len(station_vals) - 1:
    		station_change += abs(station_vals[k] - station_vals[k+1])
    		print "station {0} change is {1}".format(station_id,station_change)
    	hour_change[int(station_id)] = station_change #convert the station id back to integer
 #values are in the dictionary keyed on the station ID. To find the winner:
 def keywithmaxval(d):
    # create a list of the dict's keys and values; 
    v = list(d.values())
    k = list(d.keys())
    # return the key with the max value
    return k[v.index(max(v))]
#counted = collections.Counter(hour_change)
# assign the max key to max_station
max_station = keywithmaxval(hour_change)
#query the reference table for the important information about the most active station:
#query sqlite for reference information
cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
#print "With " + str(hour_change[379]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')
print "With " + str(hour_change[379]) + " bicycles coming and going in the hour between " + datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')
# this will just print out the first station in the list that has the max value. 

#You should visually inspect the data to make sure this is the case:
plt.bar(hour_change.keys(), hour_change.values())
plt.ylabel('Running Total of Differences')
plt.xlabel('Station')
#fig, ax = plt.subplots()
#ax.bar(hour_change.keys(), hour_change.values())
#ax.set_ylabel('Running Total of Differences')
#ax.set_xlabel('Station')
plt.show()
# Write a blog post about what you found. Describe why you think this station may be so busy, 
#    especially for the time you're using in your query. 
# Try to use a map to show the area around the station