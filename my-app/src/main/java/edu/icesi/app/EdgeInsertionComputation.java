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
		DoubleWritable, MapWritable, DoubleWritable>
		 {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException {
		
		vertex.getValue().print();
		
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
		//IntWritable selectedNodeId = getBroadcast("selectedNodeId");
		
    	if(vertex.getId().get() == selectedNode.getPredecessorId()){
    		MapWritable possibleNewBsDirPred = new MapWritable();
    		MapWritable successorsDeleteCosts = getAggregatedValue("sumDeleteCostForSuccessors");
    		for(Writable branchId: successorsDeleteCosts.keySet()) {
    			IntWritable branchIdInt = (IntWritable) branchId;
    			double distanceTo = vertex.getValue().getDistances()[branchIdInt.get()]; 
    			possibleNewBsDirPred.put(branchId, new DoubleWritable(distanceTo));			
    			vertex.addEdge(EdgeFactory.create(branchIdInt, new DoubleWritable(distanceTo)));
    		}
    		aggregate("sumDeleteCostForSuccessors", possibleNewBsDirPred);
    		aggregate("possibleNewBsDirPred", possibleNewBsDirPred);
    	}else if(vertex.getValue().getPredecessorId() == selectedNodeId.get() ){ //It is a direct successor
    		MapWritable bVal = new MapWritable();
    		bVal.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		aggregate("possibleNewBsDirPred", bVal);
    	}else if(vertex.getValue().getPositions()[selectedNodeId.get()] == Position.PREDECESSOR){ 
    		/*
    		 * TODO
    		 */
    		
    	}else if(messages.iterator().hasNext()){
    		System.out.println("b Value:: " + vertex.getValue().getB());
    		//DoubleWritable[] vertexBValue = new DoubleWritable[]{new DoubleWritable(vertex.getValue().getB())};
    		//DoubleArrayWritable writableVertexBValue = new DoubleArrayWritable();
//    		MapWritable writableVertexBValue = new MapWritable();
//    		writableVertexBValue.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		DoubleWritable distanceFromPred = (DoubleWritable)messages.iterator().next().get("DIST");
    		DoubleWritable newPossibleB = new DoubleWritable(vertex.getValue().getB() + distanceFromPred.get());
    		//writableVertexBValue.set(vertexBValue);
    		sendMessage(new IntWritable(vertex.getValue().getPredecessorId()),  newPossibleB);
    	}
    	
		
	}
	

}
