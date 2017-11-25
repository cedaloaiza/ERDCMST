import random

#This are the two ways of insert a node or a subtree
BREAKING_EDGE = 0
FROM_NODE = 1
insertWaysByLocation = [BREAKING_EDGE, FROM_NODE]

global C 
C = 4*[4*[0]]
C[0][1] = 5
C[0][2] = 5
C[0][3] = 3
C[1][2] = 8
C[1][3] = 9
C[2][1] = 4
C[2][3] = 5
C[3][1] = 2
C[3][2] = 6

class Node:
	'''A tree'''
	def __init__(self, descendants, id):
		self.id = id
		self.ancestor = None
		self.descendants = None
		self.setDescendands(descendants)
		'''
		self.descendants = descendants
		if(descendants is not None):
			for descendant in self.descendants:
				descendant.setAncestor( self )
		'''
		

	def setAncestor(self, ancestor):
		self.ancestor = ancestor

	def setDescendands(self, descendants):
		if descendants is None:
			pass
		else:
			for descendant in descendants:
				descendant.setAncestor( self )
			if self.descendants is None:
				self.descendants = descendants
			else:
				self.descendants = self.descendants + descendants
		


def selectRandomlyFromList( list ):
	n = len(list)
	i = random.randint(0, n-1)
	return list[i]

def feaasibleDelete(vertexTree):
	return true

def feaasibleInsert(vertexTree):
	return true

def fakeDelete(tree, vertex):
	print( "nodo: "+str(tree.id) )
	if tree.ancestor is not None:
		print( "antecesor: "+str(tree.ancestor.id) )
	else:
		print( "antecesor: "+str(tree.ancestor) )
	if tree.descendants is not None: 
		newDescendants = list(tree.descendants)
		for node in tree.descendants:
			if vertex == node.id:
				print( "encontrado despues de " + str(tree.id) )
			else:
				fakeDelete(node, vertex)

def delete( tree, vertex ):
	if tree.descendants is not None: 
		newDescendants = list(tree.descendants)
		deletedNode = None
		for node in tree.descendants:
			if vertex == node.id:
				deletedNode = node
				#Descendants of deleted node are now descendants of deleted node's ancestor 
				tree.setDescendands(node.descendants)
				print( "encontrado despues de " + str(tree.id) )
			else:
				delete(node, vertex)
		print( "descendents of " + str(tree.id) +": " + str( len(tree.descendants) ) )
		tree.descendants.remove(deletedNode) 



def main():

	node3 = Node( None, 3)
	node2 = Node( [ node3 ], 2 )
	node1 = Node( None, 1 )

	treesito = 	Node( [ node1 , node2], 0)

	#Dictionary containing all vertices on the graph classified by their facilities
	'''
	associationFacilitiesClients = {"f1" : ["c1","c2","c3","c4","c11"],
								    "f2" : ["c4","c5","c6"],
								    "f3" : ["c3","c7","c8","c9","c10"]}
	list = [("f1","c1"),("f1","c2"),("f1","c3"),("f1","c4"),("f1","c11"),("f2","c4"),("f2","c5"),("f2","c6"),
			("f3","c3"),("f3","c7"),("f3","c8"),("f3","c9"),("f3","c10")]
	'''

	associationFacilitiesClients = {0 : [1,2,3 ]}
	list = [(0,1),(0,2),(0,3)]

	print( len(list) )

	while list:
		vertexTree = selectRandomlyFromList(list)
		if feasibleDelete(vertexTree): # Why Feasible delete? Delete could be unfeasible but becomes feasible with insert
			oldLoc = getLocation( vertexTree )
			cost = computeCost() # This is the cost of the current solution
			delete(vertexTree)  #It is necesary to delete and later insert again?
			bestLoc =  oldLoc
			locations = getLocations(vertexTree)
			bestWay =  None
			for location in locations:
				for way in insertWaysByLocation:
					if( feasibleInsert(location, way) ):
						newCost = computeCost(location, way)
						if( newCost < cost ):
							cost = newCost
							bestLoc = location
							bestWay = way
			if bestWay is not None:
				insert(bestloc, bestWay, vertexTree)
		list.remove(vertexTree)


#main()
node3 = Node( None, 3)
node2 = Node( [ node3 ], 2 )
node1 = Node( None, 1 )

treesito = 	Node( [ node1 , node2], 0)
delete(treesito, 2)
fakeDelete(treesito, 3)