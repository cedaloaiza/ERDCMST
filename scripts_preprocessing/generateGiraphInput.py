import numpy as np


def create_giraph_input (matrix, name, source ):
	giraph_input = open(name + "Input.txt", "w")
	cost_matrix = matrix
	i = 0
	line = ""
	for k in cost_matrix:
		print k
		row = k
		#print "row: ", row
		#print "row len: ", len(row) 
		line = "[" + str(i) + ", ["
		j = 0
		max_distance = 0
		for val in row:
			if j != len(row) - 1:
				line += str(val) + ", "
			else:
				line += str(val) + "], "
			if (val != 1000000):
				max_distance = max(max_distance, val)
			#print "max:", max_distance
			j += 1
		if i == source:
			line += "["
			for j in range(0, len(row)):
				if j != source:
					if j != len(row) - 1:
						line += str(j) + ", "
					else:
						line += str(j) + "], "
			line += str(max_distance) + ", "
			line += str(0)  
			
			
		else:
			line += "null, "
			line += str(0) + ", "
			line += str(cost_matrix[0][i]) 
		line += "]"
		i += 1
		giraph_input.write(line + "\n")
	giraph_input.close()


def generate_random_giraph_input(size, name):
	random_matrix = np.random.rand(size, size)
	create_giraph_input(random_matrix, name, 0)

generate_random_giraph_input(10, "10")