#!/usr/bin/env python
import simplejson as json
import sys

if len(sys.argv) < 2:
	print "usage:", sys.argv[0], "<numjson>"
	sys.exit(1);

for i in range(int(sys.argv[1])):
	data = {}
	data['key'] = 'value' + str(i)
	json_data = json.dumps(data)
	print json_data
