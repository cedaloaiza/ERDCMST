import cplex
import pickle

from commonsRDCMST import create_matrix, create_edges_dictionary
import numpy as np

def createLP(source,costs,ub,lp=False,wdir=None,instance=None, sol=False):
    numNodes=len(costs)
    numANodes=numNodes-1
    maxCostE=max([costs[i][j] for i in range(numNodes) for j in range(numNodes)])
    maxCostP=maxCostE*(numNodes-1)
    bigC=2*maxCostP
    c = cplex.Cplex()
    c.objective.set_sense(c.objective.sense.minimize)

    xvarsInds=[(j,k) for j in range(numNodes) for k in range(numNodes) if j!=k]
    #axvarsInds=[(j,k)  for (j,k) in xvarsInds if (j != source and k !=source)]


    xvars=['x_'+str(j)+'_'+str(k) for (j,k) in xvarsInds]
    fvars=['f_'+str(j) for j in range(numNodes)]
    bvars=['b_'+str(j) for j in range(numNodes)]


    lenxvars=len(xvars)
    lenfvars=len(fvars)
    lenbvars=len(bvars)
    #lenaxvarsInds=len(axvarsInds)

    if sol:
        c.variables.add(obj=[costs[j][k] for (j, k) in xvarsInds], names=xvars, types=['B'] * lenxvars,
                        lb=[sol[j][k] for (j, k) in xvarsInds], ub=[sol[j][k] for (j, k) in xvarsInds])
    else:
        c.variables.add(obj=[costs[j][k] for (j, k) in xvarsInds], names=xvars, types=['B'] * lenxvars)


    c.variables.add(names = fvars,lb=[0]*lenfvars,ub=[maxCostP]*lenfvars)
    c.variables.add(names = bvars,lb=[0]*lenbvars,ub=[maxCostP]*lenbvars)


    # Constraint 1
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = ['x_'+str(j)+'_'+str(k) for j in range(numNodes) if j!=k],
                                                          val = [1]*numANodes)
                                         for k in range(numNodes) if k!=source],
                             senses = ["E"]*numANodes,
                             rhs = [1]*numANodes,
                             names = ['C1_'+str(k) for k in range(numNodes) if k!=source])

    # Constraint 2
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = ['x_'+str(source)+'_'+str(k) for k in range(numNodes) if k!=source],
                                                          val = [1]*numANodes)],
                             senses = ["G"],
                             rhs = [1],
                             names = ['C2'])

    # Constraint 3
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = xvars,
                                                          val = [1]*lenxvars)],
                             senses = ["E"],
                             rhs = [numANodes],
                             names = ['C3'])

    # Constraint 4
    # f_k -  f_j  - c*x_jk >= d_jk-c
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = ['f_'+str(k),'f_'+str(j),'x_'+str(j)+'_'+str(k)],
                                                          val = [1,-1,-bigC])
                                         for (j,k) in xvarsInds],
                             senses = ["G"]*lenxvars,
                             rhs = [costs[j][k]-bigC for (j,k) in xvarsInds],
                             names = ['C4_'+str(j)+'_'+str(k) for (j,k) in xvarsInds])


    # Constraint 5
    # b_j -  b_k  - c*x_jk >= d_jk-c
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = ['b_'+str(j),'b_'+str(k),'x_'+str(j)+'_'+str(k)],
                                                          val = [1,-1,-bigC])
                                         for (j,k) in xvarsInds],
                             senses = ["G"]*lenxvars,
                             rhs = [costs[j][k]-bigC for (j,k) in xvarsInds],
                             names = ['C5_'+str(j)+'_'+str(k) for (j,k) in xvarsInds])

    # # Constraint 6
    c.linear_constraints.add(lin_expr = [cplex.SparsePair(ind = ['f_'+str(i),'b_'+str(i)],
                                                          val = [1,1])
                                         for i in range(numNodes) if i!=source],
                             senses = ["L"]*numANodes,
                             rhs = [ub]*numANodes,
                             names = ['C6_'+str(i) for i in range(numNodes) if i!=source])



    if lp:
        c.write(wdir+instance+'.lp')
    else:
        try:
            c.solve()
            return c.solution.get_objective_value()

        except:
            return 'unfeasible COP'

def readSolution(name):
    f = open(name, "r")
    lines = f.readlines()
    #print "lines: ", len(lines)
    dic = {}
    for line in lines:
        arr = line.split()
        dic[(int(arr[0]), int(arr[1]))] = 1 
    #print 'dictionary: ', dic
    f.close()
    return dic

def test(test_name, upper_bound):
    solution = readSolution(test_name + "Output")
    #edges = pickle.load(open(test_name + "Edges.pk"))
    edges = create_edges_dictionary(test_name, {})
    costs = create_matrix(len(solution) + 1, edges, 1000000)  
    #print "edges:", edges
    #print "costs:", costs
    sol = create_matrix(len(costs[0]), solution, defaultValue=0)
    print createLP(0, costs, upper_bound, sol=sol)
    print createLP(0, costs, upper_bound)

def RDCMST_cplex_random(size):
    import networkx as nx
    np.random.seed(1)
    random_matrix = np.random.rand(size, size)
    for i in range(len(random_matrix)):
        random_matrix[i][i] = 0
    g = nx.from_numpy_matrix(random_matrix) 
    mst = nx.minimum_spanning_tree(g)
    print g.edges()
    print nx.get_edge_attributes(mst, 'weight').values()
    mst_cost = sum(nx.get_edge_attributes(mst, 'weight').values())
    print "MST cost: " + str(mst_cost)
    initial_solution = sum(random_matrix[0])
    print 'Initial solution: ' + str(initial_solution)
    source_b = max(random_matrix[0])
    print "Source's b :"  + str(source_b)
    upper_bound = (source_b + mst_cost) / 2
    print 'Upper bound: ' + str(upper_bound)
    print createLP(0, random_matrix, upper_bound)
    print random_matrix
    


#sampleArcs = {(0,1):5,(0,2):5,(0,3):3,(1,2):8,(1,3):9,(2,1):4,(2,3):5,(3,1):2,(3,2):6}
#sampleMatrix = createMatrix(4,sampleArcs)
#pickle.dump(sampleMatrix, open("test1Costs.pk", "w"))

#arcs1={(0,1):3,(0,2):4,(1,3):2,(1,5):5,(2,3):10,(2,4):1,(2,5):2}

#arcs2={(0,1):1,(0,2):1,(1,3):1,(2,4):1,(2,5):1}


#costs=createMatrix(6,arcs1)
#sol=createMatrix(6,arcs2,defaultValue=0)

#print 'CALL 0 ', createLP(0,costs,100)
#print 'CALL 1 ', createLP(0,costs,3)
#print 'CALL 2 ', createLP(0,costs,100,sol=sol)
#print 'CALL 3 ', createLP(0,costs,3,sol=sol)


#test("test1", 100)
#test("../files/spain_locsEuclidean", 100)

'''
size = 10
upper_bound = 1000
np.random.seed(1)
random_matrix = np.random.rand(size, size)
print createLP(0, random_matrix, upper_bound)
import networkx as nx
g = nx.from_numpy_matrix(random_matrix, create_using=nx.DiGraph()) 
mst = nx.minimum_spanning_tree(g)
print g.edges()
print nx.get_edge_attributes(mst, 'weight').values()
print "MST cost: " + str(sum(nx.get_edge_attributes(mst, 'weight').values()))
'''
RDCMST_cplex_random(1000)