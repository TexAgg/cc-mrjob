import re
from collections import Counter
from mrcc import CCJob
from bs4 import BeautifulSoup
import itertools
import warc
import csv

# I don't need Hadoop for this lol.
def scrape():
	# http://bit.ly/2pOfwkm
	csvfile = open('out/data.csv', 'w')
	fieldnames = ['url', 'content']
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()

	f = warc.open("crawl-data/CC-MAIN-2014-35/segments/1408500800168.29/warc/CC-MAIN-20140820021320-00000-ip-10-180-136-8.ec2.internal.warc.gz")
	for record in f:
		process(record, writer)
	csvfile.close()

def process(record, writer):
	if record['Content-Type'] == 'application/http; msgtype=response':
		payload = record.payload.read()
		headers, body = payload.split('\r\n\r\n', 1)
		if 'Content-Type: text/html' in headers:
			codes = get_code(body)
			for c in codes:
				print record['WARC-Target-URI']
				writer.writerow({"url": record['WARC-Target-URI'], 'content': c.encode('utf-8')})

def get_code(data, ctr=None):
	if ctr is None:
		ctr = []
	# http://bit.ly/2qhuYIp
	soup = BeautifulSoup(data, 'html.parser')
	for code in soup.find_all('pre'):
		ctr.append(code.get_text())
	for code in soup.find_all('code'):
		ctr.append(code.get_text())
	return ctr

class CodeGetter(CCJob):
	def process_record(self, record):
		# WARC records have three different types:
		#  ["application/warc-fields", "application/http; msgtype=request", "application/http; msgtype=response"]
		# We're only interested in the HTTP responses
		if record['Content-Type'] == 'application/http; msgtype=response':
			payload = record.payload.read()
			# The HTTP response is defined by a specification: first part is headers (metadata)
			# and then following two CRLFs (newlines) has the data for the response
			headers, body = payload.split('\r\n\r\n', 1)
			if 'Content-Type: text/html' in headers:
				codes = get_code(body)
				urls = [record['WARC-Target-URI']]
				data = zip(itertools.cycle(urls), codes)
				for key, value in data:
					print record['WARC-Target-URI']
					yield key, value
				self.increment_counter('commoncrawl', 'processed_pages', 1)

if __name__ == "__main__":
	#CodeGetter.run()
	scrape()