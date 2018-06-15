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
		
		vertex.getValue().print();
		RDCMSTValue selectedNode = getBroadcast("selectedNode");
		
		if (vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR) {
			/* TODO
			 * 
			 */
		}
		
		if(messages.iterator().hasNext()){
			MapWritable receivedMessage = messages.iterator().next();
			Location partialBestLocation = computeCostInsertingBreakingEdge(vertex, selectedNode, receivedMessage);
			if(partialBestLocation != null){
				aggregate("bestLocation", partialBestLocation);
			}
		}
		
		sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), vertex.getId());
		
	}
	
	/**
	 * compute the cost of inserting the removing node FROM NODE WAY in each node.
	 * @param vertex
	 * @param selectedNode
	 */
	public Location computeCostInsertingBreakingEdge(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode, MapWritable message) {
		DoubleWritable predecessorF = (DoubleWritable) message.get(new Text("F"));
		DoubleWritable predecessorToSelectedNode = (DoubleWritable) message.get(new Text("TO_SELEC"));
		DoubleWritable selectedNodeToHere = (DoubleWritable) message.get(new Text("TO_SUCC"));
		Location partialBestLocation = null;
		//feasible insert
		boolean feasibleInsert = (predecessorF.get() + predecessorToSelectedNode.get()  + selectedNodeToHere.get() + vertex.getValue().getB()) <= 10;
		if (feasibleInsert) {	
			double costBE =  vertex.getValue().getDistances()[selectedNode.getId()] + selectedNodeToHere.get() - selectedNodeToHere.get();
			double costFN = vertex.getValue().getPartialBestLocationCost();
			if (costBE < costFN) {
				partialBestLocation = new Location(vertex.getId().get(), Way.BREAKING_EDGE, costBE, vertex.getValue().getPredecessorId());
			} else {
				partialBestLocation = new Location(vertex.getId().get(), Way.FROM_NODE, costFN, vertex.getValue().getPredecessorId());
			}	
		}
		
		return partialBestLocation;
		
	}

}
