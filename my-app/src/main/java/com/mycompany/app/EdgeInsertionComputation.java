package com.mycompany.app;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class EdgeInsertionComputation extends BasicComputation
		<IntWritable, RDCMSTValue,
		DoubleWritable, MapWritable> {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException {
		
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
		//IntWritable selectedNodeId = getBroadcast("selectedNodeId");
		
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
    	}else if(vertex.getValue().getPositions()[selectedNodeId.get()] == Position.PREDECESSOR){ 
    		System.out.println("b Value:: " + vertex.getValue().getB());
    		//DoubleWritable[] vertexBValue = new DoubleWritable[]{new DoubleWritable(vertex.getValue().getB())};
    		//DoubleArrayWritable writableVertexBValue = new DoubleArrayWritable();
    		MapWritable writableVertexBValue = new MapWritable();
    		writableVertexBValue.put(vertex.getId(), new DoubleWritable(vertex.getValue().getB()));
    		//writableVertexBValue.set(vertexBValue);
    		sendMessage(new IntWritable(vertex.getValue().getPredecessorId()),  writableVertexBValue);
    	}
    	
		
	}
	

}
