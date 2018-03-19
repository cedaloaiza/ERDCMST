package com.mycompany.app;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

/**
 * Update b and f after the delete.
   Compute the cost of inserting from node.
   Send messages to complete information

   In this point, we have enough information to update the b and f values of every node in the graph.
   Depending on the node, it updates its state in one way or another. 
   If the node was a predecessor of the removing node, it has to update its b value with predecessorsDeleteCost,
   only in the case where it does not have a different branch with a farther leaf.
   If the node was a successor of  the removing node, it has to update its f value according to
   the value in aggrtrAddDeleteCosts  which matches with the branch where the node is.
   If the node is neither a predecessor nor a successor of the removing node, nothing happens. 
 * @author cdlq1
 *
 */
public class BFsUpdateAndBestLocationBeginningComputation extends BasicComputation
	<IntWritable, RDCMSTValue,
	DoubleWritable, MapWritable> {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException {
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
		MapWritable message = messages.iterator().next();
		
		updateBnFs(vertex, selectedNode, message);
	}
	
	public void updateBnFs(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode,
			MapWritable message){
		
		IntWritable succesorId = (IntWritable) message.keySet().iterator().next();
		DoubleWritable succesorB = (DoubleWritable) message.get(succesorId);
		PredecessorsDeleteCost predecessorsDeleteCost = getBroadcast("predecessorsDeleteCost");
		
		if(vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR ){
			double newPossibleLongestPath = succesorB.get() + predecessorsDeleteCost.getPredeccesorsCost() + 
					vertex.getValue().getDistances()[succesorId.get()];
			if( newPossibleLongestPath > vertex.getValue().getB() ){				
				double newB = vertex.getValue().getB() + predecessorsDeleteCost.getPredeccesorsCost();
				vertex.getValue().setB(newB);
			}
		}
	}
	
	

}