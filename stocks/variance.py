#!/usr/bin/env python


import pymongo, urllib2, datetime
from bson.objectid import ObjectId
import time
from math import sqrt
'''
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-s", "--symbol", dest="symbol", help="symbol to analyze")
parser.add_option("-l", "--limit", dest="limit", help="limit of days")
(options, args) = parser.parse_args()
symbol = options.symbol
limit = int(options.limit)
'''

symbol = 'A'
limit = 30


connection = pymongo.Connection()

db = connection["stocks"]
stocks = db["stocks"]
history = db["history"]

start_time = time.time()


cursor = stocks.find({"symbol" : symbol, "average_open" : {"$exists" : "true"}})
#cursor = stocks.find({"average_open" : {"$exists" : "true"}})
for item in cursor:
	s = item["symbol"]
	average = float(item["average_open"])

	print "Average: " + str(item["average_open"])
	
	variance_raw = 0
	count = 0
	h_cursor = history.find({"symbol" : s}).sort("date", -1).limit(limit)
	for i in h_cursor:
		variance_raw += pow(float(i["open"]) - average, 2)
		count += 1
	
	variance = variance_raw / (count - 1)
	stocks.update({"symbol" : s}, {"$set" : {"variance" : variance}})

	print "Variance: " + str(variance)
	print sqrt(variance)


	'''
	history_list = []
	h_cursor = history.find({"symbol" : s}).sort("date", -1).limit(limit)
	for i in h_cursor:
		history_list.append(i)

	average_raw = 0
	count = 0
	for i in history_list:
		average_raw += float(i["open"])
		count += 1
	average = average_raw / count
	print "Average: " + str(average)

	variance_raw = 0
	for i in history_list:
		variance_raw += pow(float(i["open"]) - average, 2)

	variance = variance_raw / count
#	stocks.update({"symbol" : s}, {"$set" : {"variance" : variance}})

	print "Variance: " + str(variance)
	print sqrt(variance)
	'''


	'''

	map = """
		function() {
			emit(this.symbol, {open: this.open, count: 1})
		}
		"""
	
	reduce = """
		function(key, values) {
			var result = {count: 0, open: 0};
			values.forEach(function(value) {
				result.open += parseFloat(value.open);
				result.count += parseInt(value.count);
			});
			return result;
		}
		"""


	data = history.inline_map_reduce(map, reduce, query = {"symbol" : s})
#	data = history.inline_map_reduce(map, reduce)
	

	for point in data:
		mr_variance = point['value']['open'] / (point['value']['count'] - 1);
		print mr_variance
		print {"symbol" : point['_id']}, {"$set" : {"variance" : point['value']}}
#		stocks.update({"symbol" : point['_id']}, {"$set" : {"variance" : point['value']}})
	
'''

print "Time elapsed: " + str(time.time() - start_time)
