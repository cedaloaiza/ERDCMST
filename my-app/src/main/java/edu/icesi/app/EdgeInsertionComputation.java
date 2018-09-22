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
		DoubleWritable, DoubleWritable, EntryWritable>
		 {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
		
		
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
		
		vertex.getValue().print();
		
		RDCMSTValue selectedNode = getBroadcast("selectedNode");
		System.out.println("selectedNode broadcasted:: " + getBroadcast("selectedNodeId"));
		System.out.println("Selected node at superstep 1: " + selectedNode.getId());
    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
		//IntWritable selectedNodeId = getBroadcast("selectedNodeId");
    	System.out.println("Selected node's parent: " + selectedNode.getPredecessorId());
		
    	if (vertex.getValue().getPositions()[selectedNodeId.get()] == Position.PREDECESSOR) { 
    		if (vertex.getValue().getPredecessorId() != RDCMSTValue.NONE_PARENT ) {
    			System.out.println("Sending ID message to its parent");
    			EntryWritable message = new EntryWritable(new Text("ID"), vertex.getId());
    			sendMessage(new IntWritable(vertex.getValue().getPredecessorId()),  message);
    		}
    		double movementCost = 0;
	    	if (vertex.getId().get() == selectedNode.getPredecessorId()) {
	    		MapWritable possibleNewBsDirPred = new MapWritable();
	    		MapWritable successorsCost = new MapWritable();
	    		MapWritable successorsDeleteCosts = getAggregatedValue("sumDeleteCostForSuccessors");
	    		System.out.println("Length of KeySet to delete insertion: " + successorsDeleteCosts.keySet().size());
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
    		System.out.println("Aggregating possibleNewBsDirPred. key:" + vertex.getId() + " value: " + vertex.getValue().getB());
    		aggregate("possibleNewBsDirPred", bVal);
    	} else if (vertex.getId().get() != selectedNode.getId() && messages.iterator().hasNext()) {
    		System.out.println("b Value:: " + vertex.getValue().getB());
    		//DoubleWritable[] vertexBValue = new DoubleWritable[]{new DoubleWritable(vertex.getValue().getB())};
    		//DoubleArrayWritable writableVertexBValue = new DoubleArrayWritable();
//    		MapWritable writableVertexBValue = new MapWritable();
//    		writableVertexBValue.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		DoubleWritable distanceFromPred = (DoubleWritable) messages.iterator().next();
    		System.out.println("The distance: " + vertex.getId() + ": " + distanceFromPred);
    		DoubleWritable newPossibleB = new DoubleWritable(vertex.getValue().getB() + distanceFromPred.get());
    		//writableVertexBValue.set(vertexBValue);
    		System.out.println("New possible b of " + vertex.getId() + ": " + newPossibleB);
    		EntryWritable message = new EntryWritable(new Text("POSSB"), newPossibleB);
    		//TupleWritable message = new TupleWritable(new Writable[] {vertex.getId(), newPossibleB});
    		sendMessage(new IntWritable(vertex.getValue().getPredecessorId()),  message);
    	}
    	
    	
    	//JUST FOR DEBUGGING
//    	System.out.println("Selected node at the end of superstep 1: " + selectedNode.getId());
		
	}
	

}
