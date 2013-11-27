#!/usr/bin/env ruby


require 'rubygems'
require 'nokogiri'
require 'mongo'
require 'open-uri'


#url_prefix = ARGV[0]
#first_url = ARGV[1]
first_url = ARGV[0]


connection = Mongo::Connection.new
db = connection['products']
products = db['products']

date_today = Time.new
date_one_year = (60 * 60 * 24 * 365)
date_start = date_today - date_one_year - date_one_year
date_start_string = date_start.strftime("&a=%m&b=%d&c=%Y")
date_end_string = date_today.strftime("&d=%m&e=%d&f=%Y")


trap("SIGINT") { throw :ctrl_c }
catch :ctrl_c do
	begin
		pages = 0
		
		def parseAndSave(url, db, pages)
			date_start = Time.new

			url_prefix = "http://www.amazon.com"
			objects = []
			doc = Nokogiri::HTML(open(url))
			doc.css('div.result').each do |el|
				title_element = el.css('a.title').first
				title = title_element.content
				author = el.css('.ptBrand a').empty? ? el.css('.ptBrand').empty? ? '' : el.css('.ptBrand').first.content.gsub!(/by /, '') : el.css('.ptBrand a').first.content
				price = el.css('td.toeOurPrice a').empty? ? el.css('td.toeOurPriceWithRent a').empty? ? "" : el.css('td.toeOurPriceWithRent a').first.content : el.css('td.toeOurPrice a').first.content
				image = el.css('img.productImage').attribute 'src'
				link = url_prefix + (title_element.attribute 'href').to_s

				objects.push({"title" => title.to_s, "author" => author.to_s, "price" => price.to_s.gsub(/[$]/, ''), "image" => image.to_s, "link" => link})
			end
			pages += 1
			db.insert(objects)
			puts "Page #{pages}: inserted #{objects.length} objects (#{Time.now - date_start} seconds)"

			next_link = doc.css('a#pagnNextLink')
			if !next_link.empty?
				next_url = doc.css('a#pagnNextLink').attribute 'href'
				next_url = url_prefix + next_url
				parseAndSave(next_url, db, pages)
			else
				puts "done"
			end
		end

		parseAndSave(first_url, products, pages)

	rescue StandardError, SystemExit, Interrupt
		raise
	rescue Exception => e
		puts "ended / errored"
	end
end

puts "Time elapsed #{Time.now - date_today} seconds"
