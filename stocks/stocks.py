#!/usr/bin/env python


import pymongo, urllib2, datetime
from bson.objectid import ObjectId
import csv, StringIO
import time
#from optparse import OptionParser
#parser = OptionParser()
#parser.add_option("-l", "--limit", dest="limit", help="limit of days")
#(options, args) = parser.parse_args()
#limit = int(options.limit)


connection = pymongo.Connection()

db = connection["stocks"]
indexes = db["indexes"]
stocks = db["stocks"]
history = db["history"]

start_time = time.time()

date_today = datetime.date.today()
date_one_year = datetime.timedelta(weeks=52)
date_one_day = datetime.timedelta(days=2)
#date_start = date_today - date_one_year - date_one_year
date_start = date_today - date_one_day
date_start_string = date_start.strftime("&a=%m&b=%d&c=%Y")
date_end_string = date_today.strftime("&d=%m&e=%d&f=%Y")

#cursor = db.stocks.find().sort("symbol", 1).limit(limit)
cursor = db.stocks.find().sort("symbol", 1)
for item in cursor:
	symbol = item["symbol"]
	print symbol

	url = "http://ichart.finance.yahoo.com/table.csv?s=" + symbol + date_start_string + date_end_string + "&g=d&ignore=.csv"
	try:
		response = urllib2.urlopen(url)
		csv_str = StringIO.StringIO(response.read())
		csv_data = csv.reader(csv_str)
		fields = csv_data.next()

		for row in csv_data:
			history.insert({"id": symbol + "_" + row[0], "symbol" : symbol, "date" : row[0], "open" : row[1], "high" : row[2], "low" : row[3], "close" : row[4], "volume" : row[5]})
	except Exception:
		print "--failed (" + url + ")"

print "Time elapsed: " + str(time.time() - start_time)
