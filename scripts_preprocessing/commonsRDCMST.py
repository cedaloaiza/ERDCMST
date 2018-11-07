def create_matrix(n, arcs, defaultValue=100):
    nodes=range(n)
    cost={}
    for i in nodes:
        cost[i]=[defaultValue]*n
        for j in nodes:
            if i==j:
                cost[i][j]=0

    for (i,j) in arcs:
        cost[i][j]=arcs[(i,j)]

    return cost



def create_edges_dictionary (input_name, ids_map, ext="csv"):
    csv_file = open(input_name + "." + ext, "r")
    csv_file.readline()
    csv_data = csv_file.readlines()
    id_consecutive = 0
    edges_dict = {}

    for line in csv_data:
        row = line.rstrip().split(",") 
        if not row[0] in ids_map:
            ids_map[row[0]] = id_consecutive
            id_consecutive += 1
        if not row[1] in ids_map:
            ids_map[row[1]] = id_consecutive
            id_consecutive +=1
        edges_dict[(ids_map[row[0]], ids_map[row[1]])] = float(row[2])

    #print edges_dict
    #print ids_map
    #pickle.dump(edges_dict, open(input_name + "Edges.pk", "w"))
    #print "Edges created"
    return edges_dict