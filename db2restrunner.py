import MySQLdb as mdb
import sys
import time
import urllib2
from ConfigParser import SafeConfigParser

parser = SafeConfigParser()
parser.read('db2restrunner.ini')

print parser.get('variables', 'name')

host = parser.get('database', 'host')
username = parser.get('database', 'username')
password = parser.get('database', 'password')
database = parser.get('database', 'database')

conn = mdb.connect( host, username, password, database )

with conn:
	cur = conn.cursor()
	sql = parser.get('database', 'sql')
	cur.execute(sql)
	
	rows = cur.fetchall()
	
	url = parser.get('restendpoint', 'url')
	
	uri = parser.get('restendpoint', 'uri')
	
	for row in rows:
		configParams = parser.get('restendpoint', 'params')
		configParams = configParams.split(',')
		urlParams = ''
		for param in configParams:
			paramArray = param.split(':')
			paramVal = ''
			isInStr = 'row['
			if( isInStr in paramArray[1].strip() ):
				paramVal = eval( paramArray[1].strip() )
			else:
				paramVal = paramArray[1].strip()
			urlParams += '%s=%s&' % (paramArray[0].strip(), paramVal.strip() )
		urlParams = urlParams[:-1]
		params = "?%s" % urlParams
		mainUrl = url + uri + params
		if( parser.get( 'variables', 'debug' ) == "1" ):
			print mainUrl
		try:
			data = urllib2.urlopen(mainUrl).read()
			if( parser.get( 'variables', 'debug' ) == "1" ):
				print data
		except urllib2.HTTPError, e:
			print "HTTP error: %d" % e.code
		except urllib2.URLError, e:
			print "Network error: %s" % e.reason.args[1]
		if( parser.has_option('variables', 'sleep') ):
			time.sleep( parser.getfloat('variables', 'sleep') )