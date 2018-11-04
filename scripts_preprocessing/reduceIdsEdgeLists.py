def reduce_vertex_ids(path_file, quantity=1):
	edge_lists = open(path_file, 'r')
	edge_out = open("edgelist_fixed", 'w')
	for line in edge_lists:
		edge = line.split(' ')
		src = int(edge[0]) - 1
		trgt = int(edge[1]) - 1
		weight = edge[2]
		out_line = str(src) + " " + str(trgt) + " " + weight
		edge_out.write(out_line)
	edge_lists.close()
	edge_out.close()

edge_lists_file ='files/edgelists_reapired_mst_spain_complete'
reduce_vertex_ids(edge_lists_file)