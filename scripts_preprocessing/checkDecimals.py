import json
from collections import OrderedDict





def check_distances(giraph_file):
	giraph_input = open(giraph_file, "r")
	for line in giraph_input:
		json_line = json.loads(line, object_pairs_hook=OrderedDict)
		source = json_line[0]
		target = 7809
		if source == 7793:
			print str(source) + ' to ' + str(target)
			print json_line[1][target]


check_distances("/home/cesardlq/spain_euc_complete_new.txt")