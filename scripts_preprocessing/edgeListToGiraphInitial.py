import json
import networkx as nx

#edge_list_file = "simpleEdgeList.txt"
#edge_list_file = "bkrus_initial_solution"

#G=nx.read_weighted_edgelist(edge_list_file)

#print G.get_edge_data('0','12544')['weight'] 
#for i in range(0,1200):

#print len(G.edges())
#print nx.shortest_path_length(G, '0', '61')

def create_graph_from_paths(paths, G):
	di_tree = nx.DiGraph()
	for path in paths:
		i = 0
		previous_v = None
		for v in path:
			if i != 0:
				w = G.get_edge_data(previous_v, v)['weight'] if G.has_edge(previous_v, v) else G.get_edge_data(v, previous_v)['weight']
				di_tree.add_edge(previous_v, v, weight=w)
			previous_v = v
			i += 1
	return di_tree




#es = di_tree.edges()
#print len(es)
#print nx.shortest_path_length(di_tree, '0', '1')

#print sum(nx.get_edge_attributes(di_tree, 'weight').values())

def reduce_vertex_ids(path_file, quantity=1):
	edge_lists = open(path_file, 'r')
	edge_out = open("edgelist_fixed", 'w')
	for line in edge_lists:
		edge = line.split(' ')
		src = int(edge[0]) - 1
		trgt = int(edge[1]) - 1
		weight = edge[2]
		out_line = str(src) + " " + str(trgt) + " " + weight
		edge_out.write(out_line + "\n")
	edge_lists.close()
	edge_out.close()

def edgelist_to_digraph(path_file, type_solution='bkrus'):
	di_tree = None
	if type_solution == 'bkrus':
		G=nx.read_weighted_edgelist(path_file)
		paths_from_source = nx.single_source_dijkstra_path(G, '0')
		di_tree = create_graph_from_paths(paths_from_source.values(), G)
	elif type_solution == 'repaired':
		di_tree=nx.read_weighted_edgelist(path_file, create_using=nx.DiGraph())
	print "Tree cost: " + str(sum(nx.get_edge_attributes(di_tree, 'weight').values()))
	return di_tree




def initial_solution_giraph(giraph_file, di_tree):
	giraph_input = open(giraph_file, "r")
	giraph_out = open('giraph_input_bkrus.txt', 'w')
	for line in giraph_input:
		json_line = json.loads(line)
		vertex = str(json_line[0])
		sons =  di_tree.neighbors(vertex)
		f = nx.shortest_path_length(di_tree, '0', vertex, 'weight')
		b = max(nx.shortest_path_length(di_tree, vertex, weight='weight').values())
		json_line[2] = [int(x) for x in sons]
		json_line[3] = float(b)
		json_line[4] = float(f)
		out_line = str(json_line)
		giraph_out.write(out_line + '\n')
	giraph_out.close()
	giraph_input.close()




giraph_input ='/grid/bigInput/spain_euc_complete_new.txt'
edge_lists_file ='/home/client/edgelists_reapired_mst_spain_complete'
reduce_vertex_ids(edge_lists_file)
#di_tree = edgelist_to_digraph(edge_lists_file, type_solution='repaired')
#initial_solution_giraph(giraph_input, di_tree)
#initial_solution_giraph("/home/cesardlq/spain_euc_complete_new.txt")

'''
paths = [[0, 1, 2, 3], [0, 2, 8, 9, 10], [0, 3, 8, 9, 15]]
di_tree = create_graph_from_paths(paths)
print di_tree.edges()
single_source_dijkstra_path(G)
'''
