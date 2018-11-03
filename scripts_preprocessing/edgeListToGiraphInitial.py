import json
import networkx as nx

edge_list_file = "simpleEdgeList.txt"
#edge_list_file = "bkrus_initial_solution"

G=nx.read_weighted_edgelist(edge_list_file)

#print G.get_edge_data('0','12544')['weight'] 
#for i in range(0,1200):

print len(G.edges())
#print nx.shortest_path_length(G, '0', '61')

def create_graph_from_paths(paths):
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


paths_from_source = nx.single_source_dijkstra_path(G, '0')
di_tree = create_graph_from_paths(paths_from_source.values())
es = di_tree.edges()
print len(es)
print nx.shortest_path_length(di_tree, '0', '1')

print sum(nx.get_edge_attributes(di_tree, 'weight').values())





def initial_solution_giraph(giraph_file):
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



initial_solution_giraph("exampleRDCMST.txt")
#initial_solution_giraph("/home/cesardlq/spain_euc_complete_new.txt")

'''
paths = [[0, 1, 2, 3], [0, 2, 8, 9, 10], [0, 3, 8, 9, 15]]
di_tree = create_graph_from_paths(paths)
print di_tree.edges()
single_source_dijkstra_path(G)
'''