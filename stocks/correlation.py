#!/usr/bin/env python


import pymongo, urllib2, datetime
from bson.objectid import ObjectId
import time
from math import sqrt
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-l", "--limit", dest="limit", help="limit of days")
parser.add_option("-f", "--symbol1", dest="symbol1", help="symbol to analyze")
parser.add_option("-s", "--symbol2", dest="symbol2", help="symbol to analyze against")
(options, args) = parser.parse_args()
limit = int(options.limit)
s1 = options.symbol1
s2 = options.symbol2


connection = pymongo.Connection()

db = connection["stocks"]
stocks = db["stocks"]
history = db["history"]
covar = db["covariance"]

start_time = time.time()


count = 0
cursor = stocks.find({"average_open" : {"$exists" : "true"}, "variance" : {"$exists" : "true"}, "symbol" : s1}).sort("symbol", 1)
for item in cursor:
	symbol = item["symbol"]
	average = float(item["average_open"])
	variance = float(item["variance"])

	history_cursor = history.find({"symbol" : symbol}).sort("date", -1).limit(limit)
	cursor2 = stocks.find({"average_open" : {"$exists" : "true"}, "variance" : {"$exists" : "true"}, "symbol" : s2}).sort("symbol", 1) #.skip(count)

	for item2 in cursor2:
		symbol2 = item2["symbol"]
		average2 = float(item2["average_open"])
		variance2 = float(item2["variance"])

		history_cursor2 = history.find({"symbol" : symbol2}).sort("date", -1).limit(limit)

		covar = 0;
		length = 0;

		for a, b in zip(history_cursor, history_cursor2):
			covar += ((float(a['open']) - average) * (float(b['open']) - average2))
			length += 1

	covariance = covar / length


	print "Covariance: " + str(covariance)

#	print variance
#	print variance2

	root = sqrt(variance)
	root2 = sqrt(variance2)

#	print root
#	print root2

	print "Correlation: " + str(covariance / (root * root2))

#	stocks.update({"symbol" : point['_id']}, {"$set" : {"variance" : point['value']}})
	
	count += 1;


print "Time elapsed: " + str(time.time() - start_time)
