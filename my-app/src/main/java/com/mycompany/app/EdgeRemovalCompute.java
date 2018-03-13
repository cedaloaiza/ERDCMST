package com.mycompany.app;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.Iterables;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class EdgeRemovalCompute extends
        BasicComputation<IntWritable, RDCMSTValue,
        FloatWritable, Text> {
   
	public void compute(Vertex<IntWritable, RDCMSTValue,
    		FloatWritable> vertex, Iterable<Text> iterable) throws IOException {
    	
    	System.out.println("node:: " + vertex.getId());
    	System.out.println("selectedNode:: " + getBroadcast("selectedNodeId"));
    	boolean equal = vertex.getId().equals(getBroadcast("selectedNodeId"));
    	System.out.println("are they equal:: " + equal );
    	

    	if(vertex.getId().equals(getBroadcast("selectedNodeId"))){
    		System.out.println("b::: " + vertex.getValue().getB());
    		System.out.println("Length Distances:: " + vertex.getValue().getDistances().length);
    		System.out.println("PredID:: " + vertex.getValue().getPredecessorId());
    		System.out.println(":: Computing node " + vertex.getId() );
    		aggregate("selectedNode", vertex.getValue());
    		MapWritable vertexSuccessors = new MapWritable();
    		for(Edge<IntWritable, FloatWritable> edge: vertex.getEdges()){  			
    			vertexSuccessors.put(edge.getTargetVertexId(), new DoubleWritable(vertex.getValue().getDistances()[edge.getTargetVertexId().get()]));
    		}
    		reduce("addDeleteCosts", vertexSuccessors);
    	}
    	
        sendMessageToAllEdges(vertex, new Text());
        
    }
}
