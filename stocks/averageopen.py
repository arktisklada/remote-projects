#!/usr/bin/env python


import pymongo, urllib2, datetime
from bson.objectid import ObjectId
import time
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-s", "--symbol", dest="symbol", help="symbol to analyze")
parser.add_option("-l", "--limit", dest="limit", help="limit of days")
(options, args) = parser.parse_args()
if options.symbol:
	symbol = options.symbol
if options.limit:
	limit = int(options.limit)


connection = pymongo.Connection()

db = connection["stocks"]
stocks = db["stocks"]
history = db["history"]

start_time = time.time()

'''

stock_cursor = stocks.find({"symbol" : {"$regex" : "^X"}, "average_open" : {"$exists" : "true"}}).sort("symbol", 1)
for stock_item in stock_cursor:
	symbol = stock_item["symbol"]

#	cursor = db.history.find({"symbol" : symbol}).sort("date", -1).limit(limit)
	cursor = db.history.find({"symbol" : symbol}).sort("date", -1)
	sum = 0;
	count = 0;
	for item in cursor:
		o = float(item['open'])
		sum += o
		count += 1

	average = sum / count;
	stocks.update({"symbol" : symbol}, {"$set" : {"average_open" : average}})
	print symbol + ": " +  str(average)

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
			result.count += parseInt(value.count);
			result.open += parseFloat(value.open);
		});
		return result;
	}
	"""

#data = history.inline_map_reduce(map, reduce, query = {"symbol" : {"$regex" : "^Y"}})
data = history.inline_map_reduce(map, reduce)

for point in data:
	average = float(point['value']['open']) / float(point['value']['count'])
	stocks.update({"symbol" : point['_id']}, {"$set" : {"average_open" : average}})
	print point['_id'] + ": " + str(average)



print "Time elapsed: " + str(time.time() - start_time)
