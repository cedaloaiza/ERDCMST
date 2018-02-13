import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


//NodeClass

class ANode (
	val id: Int,
	val b: Double,
	//It is var just for a test
	var f: Double,
	val dists: Array[Double],
	var cost: Double,
	var location: Tuple2[String,Double] = ("-", Double.PositiveInfinity),
	val inNode: ANode = null
	) extends Serializable {}


//Node to insert
val n4 = new ANode(4, 0, 0, Array(0,6,8,5,0), 15 )

val f0 = new ANode(0, 10, 0, Array(0,5,5,3,4), 15, inNode = n4 )
val n1 = new ANode(1, 0, 5, Array(0,0,8,9,6), 15, inNode = n4 )
val n2 = new ANode(2, 5, 5, Array(0,4,0,5,1), 15, inNode = n4 )
val n3 = new ANode(3, 0, 10, Array(0,2,6,0,6), 15, inNode = n4 )



// Create an RDD for the vertices
val nodes: RDD[(VertexId, ANode)] =
  sc.parallelize(Array((0L, f0), (1L, n1),
                       (2L, n2), (3L, n3)))

// Create an RDD for edges
val edges: RDD[Edge[Int]] =
  sc.parallelize(Array(Edge(0L, 1L, 5),    Edge(0L, 2L, 5),
                       Edge(2L, 3L, 5)))
// Define a default user in case there are relationship with missing user
val defaultUser = new ANode(0, 0, 0, null, 0)


// Build the initial Graph
val graph = Graph(nodes, edges, defaultUser)

/*
val sssp = graph.pregel(Double.PositiveInfinity)(
  (id, vertex, newDist) => {
  	val sp = math.min(vertex.f, newDist) // Vertex Program
  	vertex.f = sp
  	vertex
  },
  triplet => {  // Send Message
    if (triplet.srcAttr.f + triplet.attr < triplet.dstAttr.f) {
      Iterator((triplet.dstId, triplet.srcAttr.f + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // Merge Message
)
println(sssp.vertices.collect.mkString("\n"))
println("hola")
*/

//Pregel Approach
/*
val bestLoc = graph.pregel(Double.PositiveInfinity)(
  (id, vertex, newCost) => {
  	val cst = math.min(vertex.location._2, newCost) // Vertex Program
  	vertex.location = ("", cst)
  	vertex
  },
  triplet => {  // Send Message
  	var costBE = Double.PositiveInfinity
  	var costFN = Double.PositiveInfinity
  	var cost = Double.PositiveInfinity
  	println("HOLAAAAA")
  	//feasible insert breaking edge
  	val feasibleBE = (triplet.srcAttr.f + triplet.srcAttr.dists(triplet.srcAttr.inNode.id)  + triplet.srcAttr.inNode.dists(triplet.dstId.toInt) + triplet.dstAttr.b) <= 10
    if(feasibleBE){
    	//compute cost
    	costBE = triplet.srcAttr.dists(triplet.srcAttr.inNode.id)  + triplet.srcAttr.inNode.dists(triplet.dstId.toInt) - triplet.srcAttr.dists(triplet.dstId.toInt)
    }
    //feasible insert from node
    //#0 has to change to verex.b in treeOperator
    val feasibleFN = (triplet.dstAttr.f + triplet.dstAttr.dists(triplet.dstAttr.inNode.id) + 0) <= 10
    if(feasibleFN){
    	costFN = triplet.dstAttr.dists(triplet.dstAttr.inNode.id)
    }
    if(costBE < costFN){
    	cost = costBE
    }else if (costBE >= costFN){
		cost = costFN
    }
    
    if (cost < triplet.dstAttr.location._2) {
      Iterator((triplet.dstId, cost))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // Merge Message
)
*/

//AggregateMessages and Reduce Approach
val bestLoc = graph.aggregateMessages[(String, Double)](
  triplet => {  // Send Message
    var costBE = Double.PositiveInfinity
    var costFN = Double.PositiveInfinity
    var cost = Double.PositiveInfinity
    var insertingType = "-"
    //feasible insert breaking edge
    val feasibleBE = (triplet.srcAttr.f + triplet.srcAttr.dists(triplet.srcAttr.inNode.id)  + triplet.srcAttr.inNode.dists(triplet.dstId.toInt) + triplet.dstAttr.b) <= 10
    if(feasibleBE){
      //compute cost
      costBE = triplet.srcAttr.dists(triplet.srcAttr.inNode.id)  + triplet.srcAttr.inNode.dists(triplet.dstId.toInt) - triplet.srcAttr.dists(triplet.dstId.toInt)
    }
    //feasible insert from node
    //#0 has to change to verex.b in treeOperator
    val feasibleFN = (triplet.dstAttr.f + triplet.dstAttr.dists(triplet.dstAttr.inNode.id) + 0) <= 10
    if(feasibleFN){
      costFN = triplet.dstAttr.dists(triplet.dstAttr.inNode.id)
    }
    if(costBE < costFN){
      cost = costBE
      insertingType = "BREAKING-EDGE"
    }else if (costBE >= costFN){
      cost = costFN
      insertingType = "FROM-NODE"
    }
    if (cost < triplet.dstAttr.location._2) {
      triplet.sendToDst((insertingType, cost))
    }
  },
  // Add counter and age
  (a, b) => (a._1, b._2) // reduce function
)


//Get best location among the vertices
val bl = bestLoc.reduce((a,b) => if(a._2._2 <= b._2._2) a else b)
bestLoc.foreach(println(_))
println(bl)




/*

val t0 = System.nanoTime()
//Get a graph with the shortest paths from professors vertices to every vertex on the graph
val shortestPathsGraph = ShortestPaths.run(graph, medoids)

val t1 = System.nanoTime()
val elapsed = (t1 - t0) / 1000000
println(s"Computing time: $elapsed ms")



println(n)
*/
/*
//Reduce the shortest paths leaving just the path to the nearest professor of every vertex
val shortestPaths  = shortestPathsGraph.vertices.mapValues( (vId,mapSP) => {
	if(mapSP.nonEmpty)
		mapSP.minBy(_._2)
	else
		Map()
})



println("shortestPathsGraph vertices:")
shortestPathsGraph.vertices.foreach(println(_))
println("Kmedoids:")
shortestPaths.foreach(println(_))

*/

/*

for(v <- result.vertices){
	//println(v)
	if(v._2.nonEmpty){
		println(v._2.get)
		//println(v._2.minBy(_._2))
	}
}

result.vertices.foreach(println(_))


*/
