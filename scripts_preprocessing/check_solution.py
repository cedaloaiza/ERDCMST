import networkx as nx

def check_RDCMST(di_tree):
	print "Tree cost: " + str(sum(nx.get_edge_attributes(di_tree, 'weight').values()))
	print "Maximum distance from source: " + str(max(nx.shortest_path_length(di_tree, '0', weight='weight').values()))

#graph_file = 'files/out_complete_is_bkrus'
#graph_file = 'bkrus_initial_solution_complete'
#graph_file = 'files/edgelist_fixed_complete'
#graph_file = 'files/out_complete_is_repaired'
#graph_file = 'files/edgelist_fixed_half'
#graph_file = 'files/out_half_is_repaired'
#graph_file = 'bkrus_initial_solution_half'
#graph_file = 'files/out_half_is_bkrus'
#graph_file = 'files/out_complete_medio'
#graph_file = 'files/out_complete_left'
#graph_file = 'files/out_complete_right'
#graph_file = 'files/out_half_medio'
#graph_file = 'files/out_half_left'
#graph_file = 'files/out_OneAndAHalf_medio.txt'
#graph_file = 'files/out_oneAndAHalf_left.txt'
#graph_file = 'files/out_oneAndAHalf_right'
#graph_file = 'files/edgelist_fixed_oneAndAHalf'
graph_file = 'files/out_oneAndAHalf_is_repaired'

di_tree = nx.read_weighted_edgelist(graph_file, create_using=nx.DiGraph())
check_RDCMST(di_tree)