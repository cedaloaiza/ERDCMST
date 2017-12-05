import random
import copy

#This are the two ways of insert a node or a subtree
BREAKING_EDGE = 0
FROM_NODE = 1
insertWaysByLocation = [BREAKING_EDGE, FROM_NODE]

 
C = [ [ 0 for i in range(4) ] for j in range(4) ]
C[0][1] = 5
C[0][2] = 5
C[0][3] = 3
C[1][2] = 8
C[1][3] = 9
C[2][1] = 4
C[2][3] = 5
C[3][1] = 2
C[3][2] = 6
print("01" + str(C[0][1]))

obj = 15

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

	def isLeaf(self):
		return descendants == None

	def printTree(self, level=0):
		ancestorString = ''
		if self.ancestor is not None:
			ancestorString = str( self.ancestor.id )
		print( '\t' * level + ancestorString + "->" + str(self.id) )
		if self.descendants is not None:
		    for child in self.descendants:
		        child.printTree(level+1)
		


def selectRandomlyFromList( list ):
	n = len(list)
	i = random.randint(0, n-1)
	return list[i]

def feasibleDelete(vertexTree):
	return True

def feasibleInsert(vertexTree):
	return True

def searchNode(tree, vertex):
	print( "Searching... " )
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
				searchNode(node, vertex)

def delete( tree, vertex ):
	global obj
	if tree.descendants is not None: 
		newDescendants = list(tree.descendants)
		deletedNode = None
		for node in tree.descendants:
			if vertex == node.id:
				deletedNode = node
				#Descendants of deleted node are now descendants of deleted node's ancestor 
				tree.setDescendands(node.descendants)
				obj = obj - C[tree.id][node.id]
				if node.descendants is not None:
					reconnectingCost = 0
					for descendant in node.descendants:
						reconnectingCost = reconnectingCost + C[tree.id][descendant.id] - C[node.id][descendant.id]
					obj = obj + reconnectingCost	
				#print( "encontrado despues de " + str(tree.id) )
			else:
				delete(node, vertex)
		#print( "descendents of " + str(tree.id) +": " + str( len(tree.descendants) ) )
		if deletedNode is not None:
			tree.descendants.remove(deletedNode) 

def insert( tree, location, way, vertex ):
	inserted = False
	if tree.id == location:
		if way == FROM_NODE:
			newNode =  Node( None, vertex )
			tree.setDescendands( [newNode] )
		elif way == BREAKING_EDGE:
			newNode =  Node( [tree], vertex )
			tree.ancestor.setDescendands( [newNode] )
			tree.ancestor.descendants.remove( tree )
		return True
	elif tree.descendants is None:
		return False
	else:
		for descendant in tree.descendants:
			inserted = inserted or insert( descendant, location, way, vertex )
	return inserted


def computeCost( location, way, vertex ):
	global obj

	cost = 100
	if way == FROM_NODE:
		print( str(location.id)+ "," + str(vertex) + ": " + str(C[location.id][vertex]) )
		cost = obj + C[location.id][vertex]
	if location.ancestor is not None:
		if way == BREAKING_EDGE:
			cost = obj + C[location.ancestor.id][vertex] +  C[vertex][location.id] - C[location.ancestor.id][location.id]
	return cost  

def treeToList( tree ):
	treeList = []
	treeList.append(tree)
	if tree.descendants is not None:
		for descendant in tree.descendants:
			treeList = treeList + treeToList(descendant)
	return treeList

def main():

	global obj

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
	list = [ (0,1), (0,2), (0,3) ]

	print( len(list) )

	node3 = Node( None, 3)
	node2 = Node( [ node3 ], 2 )
	node1 = Node( None, 1 )
	tree = 	Node( [ node1 , node2], 0)

	while list:
		vertexTree = selectRandomlyFromList(list)
		vertex = vertexTree[1]
		if feasibleDelete(vertexTree): # Why Feasible delete? Delete could be unfeasible but becomes feasible with insert
			#oldLoc = getLocation( vertexTree ) For now, if none best location is found, algorithm will re insert the deleted node in the same position 
			cost = obj#computeCost() # This is the cost of the current solution
			#Maintain the tree state before the delete
			oldTree =  copy.deepcopy( tree )
			delete(tree, vertex)  #It is necesary to delete and later insert again?
			print( "After Delete: ")
			tree.printTree()
			#locations = getLocations(vertexTree)
			locations = treeToList(tree)
			bestWay =  None
			for location in locations:
				for way in insertWaysByLocation:
					if( feasibleInsert(location) ):
						newCost = computeCost(location, way, vertex)
						print( "obj " + str(obj) )
						print( "cost " + str(cost) )
						print( "newCost " + str(newCost) )
						if( newCost < cost ):
							cost = newCost
							bestLoc = location
							bestWay = way
			if bestWay is not None:
				insert(tree, bestLoc.id, bestWay, vertexTree)
			else:
				tree = oldTree
			obj =  cost
		list.remove(vertexTree)
		print( "After Iteration: ")
		tree.printTree()


main()
'''
node3 = Node( None, 3)
node2 = Node( [ node3 ], 2 )
node1 = Node( None, 1 )
treesito = 	Node( [ node1 , node2], 0)
#print( str(treeToList(treesito)) )
for e in treeToList(treesito):
	print( e.id )



treesito.printTree()

delete(treesito, 2)

treesito.printTree()
searchNode(treesito, 3)

print( treeToList(treesito) )

insert(treesito,node1.id,FROM_NODE,2)
searchNode(treesito, 2)
treesito.printTree()
'''