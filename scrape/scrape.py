#!/usr/bin/env python


import pymongo, urllib2, datetime, time
import requests, sys, cStringIO

from bson.objectid import ObjectId

from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint
import lxml
from lxml import etree, html
import lxml.html as lh

from optparse import OptionParser
parser = OptionParser()
parser.add_option("-u", "--url", dest="parse_url", help="URL to parse")
(options, args) = parser.parse_args()
if options.parse_url:
	parse_url = options.parse_url


connection = pymongo.Connection()

db = connection["products"]
products = db["products"]

start_time = time.time()



def parse(url):
#	try:

		r = requests.get(parse_url)
		root = lxml.html.fromstring(r.content)
		
#		elements = root.cssselect("div.result .data")
		elements = root.findall(".//*/a[@class='title']")
		print len(elements)
		for element in elements:
			print element.text
			print element.attrib
#			titles = element.findall(".//h3")
#			print len(titles)
#			for title in titles:
#				print str(title.attrib)




'''
		response = urllib2.urlopen(url)
		url_str = cStringIO.StringIO(response.read()).read()

		parser = etree.HTMLParser()
		tree = etree.parse(cStringIO.StringIO(url_str), parser)

		elements = tree.cssselect('div.s9h1')
		print len(elements)

		result = etree.tostring(tree.getroot(), pretty_print=True, method="html")
'''
#		url_str.seek(1,0)

#		html = lh.fromstring(url_str)
#		elements = html.cssselect('div.s9hl')
#		print len(elements)
#		for el in elements:
#			title = el.cssselect('.s9TitleText')[0].text_content()
#			price = el.cssselect('.s9Price')[0].text_content()
#			print title + ": " + price


#		parser = etree.HTMLParser()
#		tree = etree.parse(url_str, parser)
#
#		result = etree.tostring(tree.getroot(), pretty_print=True, method="html")
#		print(result)


#		tree = etree.parse(url_str)
#		print etree.tostring(tree)
#		root = tree.getroot()
#		print root.tag

'''
	except IOError as e:
		print e

	except Exception:
		print "--failed (" + url + ")"
		print sys.exc_info()[0]
'''


#parse("http://www.amazon.com/s/ref=sr_nr_n_0?rh=n%3A283155%2Cp_n_feature_browse-bin%3A2656022011%2Cn%3A%211000%2Cn%3A1&bbn=1000&ie=UTF8&qid=1351352258&rnid=1000")

parse(parse_url)



print "Time elapsed: " + str(time.time() - start_time)
sys.exit()
