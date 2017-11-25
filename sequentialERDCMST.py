import random

#This are the two ways of insert a node or a subtree
BREAKING_EDGE = 0
FROM_NODE = 1
insertWaysByLocation = [BREAKING_EDGE, FROM_NODE]

class Node:
	'''A tree'''
	def __init__(self, ancestor, descendants):
		self.ancestor = ancestor
		self.descendants = descendants


def selectRandomlyFromList( list ):
	n = len(list)
	i = random.randint(0, n-1)
	return list[i]

def feaasibleDelete(vertexTree):
	return true

def feaasibleInsert(vertexTree):
	return true


def main():
	#Dictionary containing all vertices on the graph classified by their facilities
	associationFacilitiesClients = {"f1" : ["c1","c2","c3","c4","c11"],
								    "f2" : ["c4","c5","c6"],
								    "f3" : ["c3","c7","c8","c9","c10"]}
	list = [("f1","c1"),("f1","c2"),("f1","c3"),("f1","c4"),("f1","c11"),("f2","c4"),("f2","c5"),("f2","c6"),
			("f3","c3"),("f3","c7"),("f3","c8"),("f3","c9"),("f3","c10")]
	print len(list)

	while list:
		vertexTree = selectRandomlyFromList(list)
		if feasibleDelete(vertexTree): # Why Feasible delete? Delete could be unfeasible but becomes feasible with insert
			oldLoc = getLocation( vertexTree )
			cost = computeCost() # This is the cost of the current solution
			delete(vertexTree)  #It is necesary to delete and later insert again?
			bestLoc =  oldLoc
			locations = getLocations(vertexTree)
			for location in locations:
				for 
				if( feasibleInsert(location) ):
					newCost = computeCost()
					if( newCost < cost ):
						cost = newCost
						bestLoc = location
			insert(bestloc, vertexTree)
		list.remove(vertexTree)


main()

