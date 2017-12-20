import random
import copy

#This are the two ways of insert a node or a subtree
BREAKING_EDGE = 0
FROM_NODE = 1
insertWaysByLocation = [BREAKING_EDGE, FROM_NODE]

 
C = [ [ 0 for i in range(6) ] for j in range(6) ]
C[0][1] = 5
C[0][2] = 5
C[0][3] = 3
C[1][2] = 8
C[1][3] = 9
C[2][1] = 4
C[2][3] = 5
C[3][1] = 2
C[3][2] = 6

C[4][2] = 3
C[4][5] = 5
C[4][3] = 6
C[5][2] = 7
C[5][3] = 6
C[3][5] = 8
C[2][5] = 2




obj = 29

lamb = 10

class Node:
	'''A tree'''
	def __init__(self, descendants, id):
		self.id = id
		self.f = 0
		self.b = 0
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

	def removeDescendants(self):
		self.descendants = None

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

	def printTreeVerbose(self, level=0):
		ancestorString = ''
		distance = str(0)
		if self.ancestor is not None:
			ancestorString = str( self.ancestor.id )
			distance = str( C[self.ancestor.id ][self.id] )
		print( '\t' * level + ancestorString + "--"+distance+"->" + str(self.id) )
		print( '\t' * level + "---f=" + str(self.f) )
		print( '\t' * level + "---b=" + str(self.b) )
		if self.descendants is not None:
		    for child in self.descendants:
		        child.printTreeVerbose(level+1)

    
	def setF(self, f):
		self.f = f

	def setB(self, b):
		self.b = b

	'''
	Add val to f value of all self descendants, including f value of self
	'''
	def updateFs(self, val):
		self.f = self.f + val
		if self.descendants is not None:
			for child in self.descendants:
				child.updateFs(val)

	def updateBs(self, val):
		#Debugging
		#print( "updating Bs: id:"+str(self.id)+" "+"b="+str(self.b)+" "+"val="+str(val) )
		if val >= self.b:
			self.b = val
		else:
			if self.descendants is None:
				self.b = val
			else:
				maxB = val
				for child in self.descendants:
					maxB = max( maxB, child.b + C[self.id][child.id] )
				self.b = maxB
		if self.ancestor is not None:
			self.ancestor.updateBs( self.b + C[self.ancestor.id][self.id] )

		


def selectRandomlyFromList( list ):
	n = len(list)
	i = random.randint(0, n-1)
	return list[i]

def feasibleDelete( vertex ):
	global lamb
	feasible = True
	if vertex.descendants is not None:
		for descendant in vertex.descendants:
			feasible = feasible and (vertex.ancestor.f + descendant.b + C[vertex.ancestor.id][descendant.id] <= lamb)
	else:
		feasible = True
	return feasible

#We need the deleted vertex info like b and f
def feasibleInsert(location, way, vertex):
	global lamb
	feasible = False
	if way == FROM_NODE:
		#0 has to change to verex.b in treeOperator
		feasible = (location.f + C[location.id][vertex.id] + 0) <= lamb
	elif way == BREAKING_EDGE:
		if location.ancestor is not None:
			feasible = (location.ancestor.f + C[location.ancestor.id][vertex.id]  + C[vertex.id][location.id] + location.b) <= lamb
	return feasible

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
		maxB = 0
		for node in tree.descendants:
			if vertex == node.id:
				deletedNode = node
				#Descendants of deleted node are now descendants of deleted node's ancestor 
				tree.setDescendands(node.descendants)
				obj = obj - C[tree.id][node.id]
				if node.descendants is not None:
					reconnectingCost = 0
					for descendant in node.descendants:
						#f value of descendants has to be updated with reconnection cost
						descendant.updateFs( C[tree.id][descendant.id] - C[node.id][descendant.id] - C[tree.id][node.id] )
						reconnectingCost = reconnectingCost + C[tree.id][descendant.id] - C[node.id][descendant.id]
						#b for tree has to be related with the new farthest leaf
						maxB = max(maxB,   C[tree.id][descendant.id] + descendant.b )
					obj = obj + reconnectingCost	
				#deletedNode.removeDescendants()
				#deletedNode.setAncestor(None)
				print( "{} encontrado despues de {} ".format( deletedNode.id, tree.id) )
			else:
				deleted = delete(node, vertex)
				if deleted is not None:
					return deleted
		#print( "descendents of " + str(tree.id) +": " + str( len(tree.descendants) ) )
		if deletedNode is not None:
			tree.descendants.remove(deletedNode) 
			tree.updateBs( maxB )
			return deletedNode

def insert( tree, location, way, vertex ):
	inserted = False
	if tree.id == location:
		if way == FROM_NODE:
			#newNode =  Node( None, vertex )
			tree.setDescendands( [vertex] )
			vertex.setF( tree.f + C[tree.id][vertex.id] )
			vertex.setB( 0 )
		elif way == BREAKING_EDGE:
			tree.ancestor.descendants.remove( tree )
			#newNode =  Node( None, vertex )
			tree.ancestor.setDescendands( [vertex] )
			#This is because Node constructor change the ancestor reference, so the descendants have to be set later
			vertex.setDescendands( [tree] )
			vertex.setF( vertex.ancestor.f + C[vertex.ancestor.id][vertex.id]  )
			#f value of descendant has to be updated with reconnection cost
			tree.updateFs( C[vertex.ancestor.id][vertex.id] + C[vertex.id][tree.id] - C[vertex.ancestor.id][tree.id] )
			vertex.setB( tree.b +  C[vertex.id][tree.id] )
		vertex.ancestor.updateBs( vertex.b + C[vertex.ancestor.id][vertex.id]  )
		return True
	elif tree.descendants is None:
		return False
	else:
		for descendant in tree.descendants:
			inserted = inserted or insert( descendant, location, way, vertex )
	return inserted

def insertByLocation( tree, vertex ):
	inserted = False
	if( vertex.ancestor is None ):
		raise ValueError( 'The vertex does not have an ancestor' )
	if tree.id == vertex.ancestor.id:
		if vertex.descendants is not None:
			if tree.descendants is None:
				raise ValueError( 'Descendants of the vertex are not in the tree' ) 
			else:
				newTreeDescendants = []
				newVertexDescendants = []
				for treeChild in tree.descendants:
					shouldBeTreeDescendant = True
					for vertexChild in vertex.descendants:
						if vertexChild.id == treeChild.id:
							shouldBeTreeDescendant = False
					if shouldBeTreeDescendant:
						newTreeDescendants.append( treeChild  )
					else:
						newVertexDescendants.append( treeChild )
						treeChild.updateFs( C[vertex.ancestor.id][vertex.id] + C[vertex.id][treeChild.id] - C[vertex.ancestor.id][treeChild.id] )
				tree.descendants = newTreeDescendants
				vertex.removeDescendants()
				vertex.setDescendands( newVertexDescendants )
		tree.setDescendands( [vertex] )
		inserted = True
	elif tree.descendants is None:
		inserted = False
	else:
		for descendant in tree.descendants:
			inserted = inserted or insertByLocation( descendant, vertex )
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
	node3.setF(10)
	node3.setB(0)
	node2 = Node( [ node3 ], 2 )
	node2.setF(5)
	node2.setB(5)
	node1 = Node( None, 1 )
	node1.setF(5)
	node1.setB(0)

	facilitie0 = 	Node( [ node1 , node2], 0)
	facilitie0.setF(0)
	facilitie0.setB(10)

	node22 = Node( None, 2 )
	node22.setF(3)
	node22.setB(0)
	node33 = Node( None, 3 )
	node33.setF(11)
	node33.setB(0)
	node5 = Node( [ node33 ], 5 )
	node5.setF( 5 )
	node5.setB( 6 )

	facilitie4 = Node( [ node22, node5 ], 4 )
	facilitie4.setF(0)
	facilitie4.setB(11)


	forest = { 0 : facilitie0, 4 : facilitie4 }

	print( "initial Solution: ")
	print( "Tree 1")
	facilitie0.printTreeVerbose()
	print( "Tree 2")
	facilitie4.printTreeVerbose()


	#Dictionary containing all vertices on the graph classified by their facilities
	'''
	associationFacilitiesClients = {"f1" : ["c1","c2","c3","c4","c11"],
								    "f2" : ["c4","c5","c6"],
								    "f3" : ["c3","c7","c8","c9","c10"]}
	list = [("f1","c1"),("f1","c2"),("f1","c3"),("f1","c4"),("f1","c11"),("f2","c4"),("f2","c5"),("f2","c6"),
			("f3","c3"),("f3","c7"),("f3","c8"),("f3","c9"),("f3","c10")]
	'''

	associationFacilitiesClients = {0 : [1,2,3], 4 : [2,3,5]}
	list = [ (0,node1), (0,node2), (0,node3), (4,node22), (4,node33), (4,node5)  ]


	'''
	node3 = Node( None, 3)
	node2 = Node( [ node3 ], 2 )
	node1 = Node( None, 1 )
	tree = 	Node( [ node1 , node2], 0)
	'''

	while list:
		vertexTree = selectRandomlyFromList(list)
		vertex = vertexTree[1]
		selectedFacilitie = vertexTree[0]
		tree = forest[ selectedFacilitie ]
		print( "facilitie {} and vertex {} were selected".format( selectedFacilitie, vertex.id ) )
		print( "Previous Delete: ")
		tree.printTreeVerbose()
		if feasibleDelete(vertex): # Why Feasible delete? Delete could be unfeasible but becomes feasible with insert
			#oldLoc = getLocation( vertexTree ) For now, if none best location is found, algorithm will re insert the deleted node in the same position 
			cost = obj#computeCost() # This is the cost of the current solution
			#Maintain the tree state before the delete
			oldTree =  copy.deepcopy( tree )
			#Vertex and deletedNode are the same
			deletedNode = delete(tree, vertex.id)  #It is necesary to delete and later insert again?
			print( "After Delete: ")
			tree.printTreeVerbose()
			print( "Deleted Node:" )
			vertex.printTreeVerbose()
			#locations = getLocations(vertexTree)
			locations = treeToList(tree)
			bestWay =  None
			for location in locations:
				for way in insertWaysByLocation:
					if( feasibleInsert(location, way, vertex) ):
						newCost = computeCost(location, way, vertex.id)
						print( "obj " + str(obj) )
						print( "cost " + str(cost) )
						print( "newCost " + str(newCost) )
						if( newCost < cost ):
							cost = newCost
							bestLoc = location
							bestWay = way
			if bestWay is not None:
				insert(tree, bestLoc.id, bestWay, vertex)
			else:
				tree = oldTree
			obj =  cost
		list.remove(vertexTree)
		forest[ selectedFacilitie ] = tree
		print( "After Insert: ")
		tree.printTreeVerbose()


#main()



node3 = Node( None, 3)
node2 = Node( [ node3 ], 2 )
node1 = Node( None, 1 )
treesito = 	Node( [ node1 , node2], 0)
#print( str(treeToList(treesito)) )
#for e in treeToList(treesito):
#	print( e.id )



treesito.printTreeVerbose()

delete(treesito, 2)

print('Tree after delete:')
treesito.printTreeVerbose()
print( 'Deleted Node:' )
node2.printTreeVerbose()
#searchNode(treesito, 3)

#print( treeToList(treesito) )

#insert(treesito,node1.id,FROM_NODE,2)
#insert(treesito,node1.id,BREAKING_EDGE,2)
insertByLocation( treesito, node2 )
#searchNode(treesito, 2)
print('Tree after insert:')
treesito.printTreeVerbose()
