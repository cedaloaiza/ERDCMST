package com.mycompany.app;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.Iterables;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;

public class EdgeRemovalComputation extends
        BasicComputation<IntWritable, RDCMSTValue,
        DoubleWritable, MapWritable> {
   
	public void compute(Vertex<IntWritable, RDCMSTValue,
			DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException {
    	
    	System.out.println("node:: " + vertex.getId());
    	System.out.println("selectedNode:: " + getBroadcast("selectedNodeId"));
    	boolean equal = vertex.getId().equals(getBroadcast("selectedNodeId"));
    	System.out.println("are they equal:: " + equal );
//    	
//    	RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
//    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
    	
    	IntWritable selectedNodeId = getBroadcast("selectedNodeId");

    	if(vertex.getId().equals(selectedNodeId)){
    		System.out.println("b::: " + vertex.getValue().getB());
    		System.out.println("Length Distances:: " + vertex.getValue().getDistances().length);
    		System.out.println("PredID:: " + vertex.getValue().getPredecessorId());
    		System.out.println(":: Computing node " + vertex.getId() );
    		aggregate("selectedNode", vertex.getValue());
   
    		MapWritable vertexSuccessors = new MapWritable();
    		for(Edge<IntWritable, DoubleWritable> edge: vertex.getEdges()){  			
    			vertexSuccessors.put(edge.getTargetVertexId(), new DoubleWritable(-vertex.getValue().getDistances()[edge.getTargetVertexId().get()]));
    			vertex.removeEdges(edge.getTargetVertexId());
    		}
    		reduce("addDeleteCosts", vertexSuccessors);
    		//ArrayWritable<Writable> messageSuccesorsId = new ArrayWritable<Writable>();
    		//messageSuccesorsId.set((Writable[]) vertexSuccessors.keySet().toArray());
    		removeEdgesRequest(new IntWritable(vertex.getValue().getPredecessorId()), vertex.getId());
    		//sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), messageSuccesorsId);
    	}
    	
    	
        //sendMessageToAllEdges(vertex, new Text());
        
    }
}
