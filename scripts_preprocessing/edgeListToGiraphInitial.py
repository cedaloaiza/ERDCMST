import json
import networkx as nx
import sys
sys.setrecursionlimit(1500)

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



def edgelist_to_digraph(path_file):
	di_tree = None
	G=nx.read_weighted_edgelist(path_file)
	paths_from_source = nx.single_source_dijkstra_path(G, '0')
	di_tree = create_graph_from_paths(paths_from_source.values(), G)
	print "Tree cost: " + str(sum(nx.get_edge_attributes(di_tree, 'weight').values()))
	return di_tree


def get_all_successors(di_tree, v):
	successors = []
	for child in di_tree.successors(v):
		successors.append(child)
		successors = successors + get_all_successors(di_tree, child)
	return successors

def get_positions(di_tree, v):
	positions = ['n']*di_tree.number_of_nodes()
	predecessors = nx.shortest_path(di_tree, '0', v)
	del predecessors[-1]
	for p in predecessors:
		positions[int(p)] = 's'
	successors = get_all_successors(di_tree, v)
	for s in successors:
		positions[int(s)] = 'p'
	#print 'Positions:'
	#print positions
	return positions




def initial_solution_giraph(giraph_file, di_tree):
	giraph_input = open(giraph_file, "r")
	giraph_out = open('giraph_input.txt', 'w')
	for line in giraph_input:
		json_line = json.loads(line)
		vertex = str(json_line[0])
		print vertex
		sons =  di_tree.neighbors(vertex)
		f = nx.shortest_path_length(di_tree, '0', vertex, 'weight')
		b = max(nx.shortest_path_length(di_tree, vertex, weight='weight').values())
		json_line[2] = [int(x) for x in sons]
		json_line[3] = float(b)
		json_line[4] = float(f)
		if vertex == '0':
			json_line.append(-1)
		else:
			json_line.append(int(di_tree.predecessors(vertex)[0]))
		json_line.append(get_positions(di_tree, vertex))
		#print di_tree.predecessors(vertex)
		#print get_all_successors(di_tree, vertex)
		#print get_positions(di_tree, vertex)
		#json_line[5] = float(f)
		out_line = str(json_line)
		giraph_out.write(out_line + '\n')
	giraph_out.close()
	giraph_input.close()





#giraph_input ='/home/cesardlq/spain_euc_complete_new.txt'

#giraph_input ='/home/cesardlq/spain_euc_half.txt'
giraph_input ='/home/cesardlq/spain_euc_oneAndAHalf.txt'
#giraph_input ='files/exampleRDCMST.txt'
#edge_lists_file ='files/edgelists_reapired_mst_spain_complete'
#edge_lists_file ='files/edgelist_fixed_complete'
#edge_lists_file ='files/edgelist_fixed_half'
edge_lists_file ='files/edgelist_fixed_oneAndAHalf'
#edge_lists_file ='files/simpleEdgeList.txt'
#edge_lists_file = 'bkrus_initial_solution_complete'
#edge_lists_file = 'bkrus_initial_solution_half'
#reduce_vertex_ids(edge_lists_file)
di_tree = edgelist_to_digraph(edge_lists_file)
#f = nx.shortest_path_length(di_tree, '0', '1', 'weight')
#print f
initial_solution_giraph(giraph_input, di_tree)
#initial_solution_giraph("/home/cesardlq/spain_euc_complete_new.txt")

'''
paths = [[0, 1, 2, 3], [0, 2, 8, 9, 10], [0, 3, 8, 9, 15]]
di_tree = create_graph_from_paths(paths)
print di_tree.edges()
single_source_dijkstra_path(G)
'''
