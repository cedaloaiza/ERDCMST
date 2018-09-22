package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

/**
 * Compute the cost of inserting breaking the edge.
   Get best location per node.

	We compute the cost of inserting the removing node BREAKING EDGE for each node and we decide which way is better,
	to insert FROM NODE or BREAKING EDGE. This partial best location has to be aggregated.
    The arePredsAffected flag only can be False if FROM NODE WAY is chosen and the weight of the new edge is greater than the length
    of the other paths that are born from the vId.  
    
    Finally, the b value of each node is sent to their respective predecessor.

 * @author cesardlq
 *
 */
public class BestLocationEndingComputation extends AbstractComputation
		<IntWritable, RDCMSTValue, DoubleWritable, MapWritable, IntWritable> { 
    
	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException { 
		
		
		RDCMSTValue selectedNode = getBroadcast("selectedNode");
		
		if (vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR &&
				vertex.getId().get() != selectedNode.getPredecessorId()) {
			MapWritable newBs = getBroadcast("newBs");
			DoubleWritable newB = (DoubleWritable) newBs.get(vertex.getId());
			vertex.getValue().setB(newB.get());
		}
		
		vertex.getValue().print();
		
		//PARTIAL SOLUTION
		if (vertex.getId().get() != selectedNode.getId()) {
			if (messages.iterator().hasNext()) {
				MapWritable receivedMessage = messages.iterator().next();
				Location partialBestLocation = computeCostInsertingBreakingEdge(vertex, selectedNode, receivedMessage);
				if(partialBestLocation != null){
					aggregate("bestLocation", partialBestLocation);
				}
			}
		}
		/* TODO
		 * 
		 */
		//sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), vertex.getId());
		
	}
	
	/**
	 * compute the cost of inserting the removing node FROM NODE WAY in each node.
	 * @param vertex
	 * @param selectedNode
	 */
	public Location computeCostInsertingBreakingEdge(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode, MapWritable message) {
		DoubleWritable parentToSelectedNode = (DoubleWritable) message.get(new Text("TO_SELEC"));
		DoubleWritable parentToHere = (DoubleWritable) message.get(new Text("TO_SUCC"));
		Location partialBestLocation = null;
		double costBE =  parentToSelectedNode.get() + selectedNode.getDistances()[vertex.getValue().getId()] - parentToHere.get();
		//feasible insert
		boolean feasibleInsert = (vertex.getValue().getF() + costBE + vertex.getValue().getB()) <= 100;
		System.out.println("Feasible Insert: " + feasibleInsert);
		if (feasibleInsert) {	
			//JUST FOR DEBUGGING
//			int iteration = (int) getSuperstep() / 5;
//			System.out.println("Iteration on phase 3: " + iteration);
//			double costFN = Double.POSITIVE_INFINITY;
//			if (iteration % 2 == 0) 
			double costFN = vertex.getValue().getPartialBestLocationCost();
			System.out.println("Local best location decision");
			System.out.println("FN " + costFN + " VS BE " + costBE );
			if (costBE < costFN) {
				partialBestLocation = new Location(vertex.getId().get(), Way.BREAKING_EDGE, costBE, vertex.getValue().getPredecessorId());
			} else {
				partialBestLocation = new Location(vertex.getId().get(), Way.FROM_NODE, costFN, vertex.getValue().getPredecessorId());
			}	
		}
		return partialBestLocation;
		
	}

}
