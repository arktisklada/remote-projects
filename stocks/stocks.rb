#!/usr/bin/env ruby


require 'rubygems'
require 'mongo'
require 'open-uri'


connection = Mongo::Connection.new
db = connection['stocks']
indexes = db['indexes']
stocks = db['stocks']
history = db['history']

date_today = Time.new
date_one_year = (60 * 60 * 24 * 365)
date_start = date_today - date_one_year - date_one_year
date_start_string = date_start.strftime("&a=%m&b=%d&c=%Y")
date_end_string = date_today.strftime("&d=%m&e=%d&f=%Y")


trap("SIGINT") { throw :ctrl_c }
catch :ctrl_c do
#	stocks.find.sort(:symbol).limit(100).each do |item|
	stocks.find.sort(:symbol).each do |item|
		symbol = item["symbol"]
		puts symbol
	
		url = "http://ichart.finance.yahoo.com/table.csv?s=" + symbol + date_start_string + date_end_string + "&g=d&ignore=.csv"
		begin
			open(url) do |csv_data|
				csv_data.each_line do |row|
					doc = { "id" => symbol + "_" + row[0].to_s, "symbol" => symbol, "date" => row[0], "open" => row[1], "high" => row[2], "low" => row[3], "close" => row[4], "volume" => row[5]}
					history.insert(doc)
				end
			end
		rescue Exception, StandardError, SystemExit, Interrupt
#		rescue Exception => e
#			puts e.inspect
			puts "--failed (" + url + ")"
		end
	end
end

puts "Time elapsed #{Time.now - date_today} seconds"
