from bkrus import bkrus




def loadCoordVectors(file_name):
	loc_file = open(file_name, "r")
	loc_file.readline()
	loc_data = loc_file.readlines()
	coord_vectors = []
	for line in loc_data:
		row = line.rstrip().split(",")
		coord_vectors.append([float(row[1]), float(row[2])])
	loc_file.close()
	return coord_vectors

def write_edges(edges, file_name):
	writing_file = open(file_name, "w")
	for e in edges:
		line = str(e[0]) + ' ' + str(e[1]) + ' ' + str(e[2])
		writing_file.write(line + '\n')
	writing_file.close()

from sklearn.metrics.pairwise import euclidean_distances
X = [[0, 1], [1, 1], [0, 8]]
dists = euclidean_distances(X, X)
coord_vectors = loadCoordVectors("files/spain_locs.csv")

num_partitions = 40

partition_size = len(coord_vectors) / num_partitions
partitions = []
k = 0
for i in range(num_partitions + 1):
	if i == num_partitions:
		partition_size = len(coord_vectors) % num_partitions
	partition = []
	for j in range(k, k + partition_size):
		partition.append(coord_vectors[j]) 
	partitions.append(partition)
	k = k + partition_size

partition_size = len(coord_vectors) / num_partitions
print(partitions)
print(len(partitions[0]))
print(len(partitions[39]))
print(len(partitions[40]))
print(partitions[40])

dists = euclidean_distances(partitions[0], partitions[0])
print(len(dists))

ub = 15.84738
D = []

final_tree = []
final_cost = 0 

for i in range(num_partitions + 1):
	print i
	coords = partitions[i]
	size_of_partition = len(coords)
	source = 0
	sinks = range(1, size_of_partition)
	if i != 0:
		coords.append(coord_vectors[0])
		source = size_of_partition
		sinks = range(size_of_partition)
	ED =  euclidean_distances(coords, coords)
	D = [[round(y, 5) for y in x] for x in ED]
	#print "distance from 0 to 100: " + str(D[0][100])
	#print(D)
	bkrus_solution = bkrus(source, sinks, D, ub)
	partial_tree = bkrus_solution['E']
	final_cost += bkrus_solution['cost']
	fixed_partial_tree = []
	j = 0
	for edge in partial_tree:
		#print edge
		#print edge[0]
		source_vertex = int(edge[0]) + (i * partition_size)
		target_vertex = int(edge[1]) + (i * partition_size)
		weight = float(edge[2]) 
		#print 'Partition size: ' + str(partition_size)
		#print 'Source vertex: ' + str(source_vertex)
		if edge[0] == size_of_partition:
			source_vertex = 0
			#print 'Source!!!'
		fixed_partial_tree.append((source_vertex, target_vertex, weight))
		j += 1
	#print fixed_partial_tree
	final_tree += fixed_partial_tree
	D = []
	i += 1
	print final_cost
	#print final_tree

write_edges(final_tree, "bkrus_initial_solution")