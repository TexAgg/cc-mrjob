import re
from collections import Counter
from mrcc import CCJob
from bs4 import BeautifulSoup

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
				# We avoid creating a new Counter for each page as that's actually quite slow
				codes = get_code(body)
				for c in codes:
					print record['WARC-Target-URI']
					yield record['WARC-Target-URI'], codes
				self.increment_counter('commoncrawl', 'processed_pages', 1)

if __name__ == "__main__":
	CodeGetter.run()