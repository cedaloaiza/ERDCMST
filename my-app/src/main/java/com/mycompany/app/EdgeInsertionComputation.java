package com.mycompany.app;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EdgeInsertionComputation extends BasicComputation
		<IntWritable, RDCMSTValue,
		DoubleWritable, ArrayWritable<Writable>> {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<ArrayWritable<Writable>> messages) throws IOException {
		
		IntWritable selectedNodeId = getBroadcast("selectedNodeId");
		
    	if(vertex.getId().equals(selectedNodeId)){
    		MapWritable newDeleteCosts = new MapWritable();
    		MapWritable successorsDeleteCosts = getAggregatedValue("sumSuccessorsDeleteCosts");
    		for(Writable branchId: successorsDeleteCosts.keySet()) {
    			IntWritable branchIdInt = (IntWritable) branchId;
    			double distanceTo = vertex.getValue().getDistances()[branchIdInt.get()]; 
    			newDeleteCosts.put(branchId, new DoubleWritable(distanceTo));			
    			vertex.addEdge(EdgeFactory.create(branchIdInt, new DoubleWritable(distanceTo)));
    		}
    		aggregate("sumSuccessorsDeleteCosts", newDeleteCosts);
    		aggregate("newBs", newDeleteCosts);
    	}else if(vertex.getValue().getPositions()[selectedNodeId.get()] == Position.SUCCESSOR){
    		MapWritable bVal = new MapWritable();
    		bVal.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		aggregate("newBs", bVal);
    	}
		
	}
	

}
