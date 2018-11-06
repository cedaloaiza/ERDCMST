package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.log4j.Logger;
/**
 * Now the predecessor of the removing node has the required information
 * to compute the costs of the entire delete operation. 
 * We already have the costs of removing edges, now we need to compute the cost of adding the edges 
 * between the removing node predecessor and the removing node successors. 
 * These costs must be aggregated by a sum in both aggrtrAddDeleteCosts and newBs. 
 * At the same time, the corresponding adding edges requests have to be sent. 
 * Afterward, The b values of all the successors of the removing node have to be aggregated in newBs. 
 * Finally, All the predecessors of removing node send b value to their own predecessors.
 * @author cdlq1
 *
 */
public class EdgeInsertionComputation extends AbstractComputation<IntWritable, RDCMSTValue,
		DoubleWritable, EntryWritable, DoubleWritable>
		 {
	private static final Logger LOG =
		      Logger.getLogger(EdgeInsertionComputation.class);
	
	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<EntryWritable> messages) throws IOException {
		
		
		//Completing previous phase
		MapWritable newBs = getBroadcast("newBs");
		if (!newBs.isEmpty()) {
			if (newBs.containsKey(vertex.getId())) {
				DoubleWritable newB = (DoubleWritable) newBs.get(vertex.getId());
				vertex.getValue().setB(newB.get());
			}
		}
		vertex.getValue().setOldB(vertex.getValue().getB());
		vertex.getValue().setOldF(vertex.getValue().getF());
		
		//vertex.getValue().print();
		
		RDCMSTValue selectedNode = getBroadcast("selectedNode");
		if (LOG.isDebugEnabled()) {
	          LOG.debug("selectedNode broadcasted:: " + getBroadcast("selectedNodeId"));
	          LOG.debug("Selected node at superstep 1: " + selectedNode.getId());
	          LOG.debug("Selected node's parent: " + selectedNode.getPredecessorId());
		}
    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
		//IntWritable selectedNodeId = getBroadcast("selectedNodeId");
    	
    	DoubleWritable distanceFromPred = null;
    	
    	for (EntryWritable message : messages) {
			Text messageKey = (Text) message.getKey();
			if (LOG.isDebugEnabled()) {
				System.out.println("Incoming messages. Key: " + messageKey + " Value: " + message.get(messageKey));
			}
			if (messageKey.toString().equals("ID")) {
				IntWritable childToSelectedNodeWritable = (IntWritable) message.get(messageKey);
				vertex.getValue().setIdNodeToSelectedVertex(childToSelectedNodeWritable.get());
			} else if (messageKey.toString().equals("DIST")) {
				distanceFromPred = (DoubleWritable) message.get(messageKey);
				
			}
    	}
		
    	if (vertex.getValue().getPositions()[selectedNodeId.get()] == Position.PREDECESSOR) { 
    		double movementCost = 0;
	    	if (vertex.getId().get() == selectedNode.getPredecessorId()) {
	    		MapWritable possibleNewBsDirPred = new MapWritable();
	    		MapWritable successorsCost = new MapWritable();
	    		MapWritable successorsDeleteCosts = getAggregatedValue("sumDeleteCostForSuccessors");
	    		//System.out.println("Length of KeySet to delete insertion: " + successorsDeleteCosts.keySet().size());
	    		double distanceToSelectedNode = vertex.getValue().getDistances()[selectedNode.getId()]; 
	    		movementCost -= distanceToSelectedNode;
	    		for(Writable branchId: successorsDeleteCosts.keySet()) {
	    			IntWritable branchIdInt = (IntWritable) branchId;
	    			double distanceToNewChild = vertex.getValue().getDistances()[branchIdInt.get()]; 
	    			movementCost += distanceToNewChild; 
	    			possibleNewBsDirPred.put(branchId, new DoubleWritable(distanceToNewChild));
	    			successorsCost.put(branchId, new DoubleWritable(distanceToNewChild - distanceToSelectedNode));
	    		}
	    		aggregate("sumDeleteCostForSuccessors", successorsCost);
	    		aggregate("possibleNewBsDirPred", possibleNewBsDirPred);
	    		aggregate("parentF", new DoubleWritable(vertex.getValue().getF()));
	    		aggregate("movementCost", new DoubleWritable(movementCost));
	    	}
    	} else if(vertex.getValue().getPredecessorId() == selectedNodeId.get() ){ //It is a direct successor
    		MapWritable bVal = new MapWritable();
    		bVal.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		if (LOG.isDebugEnabled()) {
  	          LOG.debug("Aggregating possibleNewBsDirPred. key:" + vertex.getId() + " value: " + vertex.getValue().getB());
    		}
    		aggregate("possibleNewBsDirPred", bVal);
    	} else if (vertex.getId().get() != selectedNode.getId() && messages.iterator().hasNext()) {
    		if (LOG.isDebugEnabled()) {
	          LOG.debug("b Value:: " + vertex.getValue().getB());
      		}
    		//DoubleWritable[] vertexBValue = new DoubleWritable[]{new DoubleWritable(vertex.getValue().getB())};
    		//DoubleArrayWritable writableVertexBValue = new DoubleArrayWritable();
//    		MapWritable writableVertexBValue = new MapWritable();
//    		writableVertexBValue.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		//System.out.println("The distance: " + vertex.getId() + ": " + distanceFromPred);
    		DoubleWritable newPossibleB = new DoubleWritable(vertex.getValue().getB() + distanceFromPred.get());
    		//writableVertexBValue.set(vertexBValue);
    		if (LOG.isDebugEnabled()) {
  	          LOG.debug("New possible b of " + vertex.getId() + ": " + newPossibleB);
    		}
    		//TupleWritable message = new TupleWritable(new Writable[] {vertex.getId(), newPossibleB});
    		sendMessage(new IntWritable(vertex.getValue().getPredecessorId()),  newPossibleB);
    	}
    	
    	
    	//JUST FOR DEBUGGING
//    	System.out.println("Selected node at the end of superstep 1: " + selectedNode.getId());
		
	}
	

}
