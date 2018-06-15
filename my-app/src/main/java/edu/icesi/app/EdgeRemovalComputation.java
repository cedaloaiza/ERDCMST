package edu.icesi.app;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.Iterables;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
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

/**
 * The removing node makes a delete request for all its edges as well as the edge that point it from its predecessor. 
 * Subsequently, the removing node stores in aggrtrAddDeleteCosts the cost of removing each one of its edges.
 * @author cdlq1
 *
 */
public class EdgeRemovalComputation extends
        AbstractComputation<IntWritable, RDCMSTValue,
        DoubleWritable, EntryWritable, MapWritable> {
   
	public void compute(Vertex<IntWritable, RDCMSTValue,
			DoubleWritable> vertex, Iterable<EntryWritable> messages) throws IOException {
		
		
		vertex.getValue().print();
		
		//JUST FOR DEBUGGING
		Location bestLocation = getAggregatedValue("bestLocation");
		System.out.println("Best Location at superstep 0:: Node:" + bestLocation.getNodeId() + " Way:" + bestLocation.getWay());
		
		//Completing the previous movement
		RDCMSTValue selectedNode = getAggregatedValue("selectedNodeA");
		if (selectedNode.getId() == vertex.getId().get()) {
			for (EntryWritable entry : messages) {
				IntWritable key = (IntWritable) entry.getKey();
				PositionWritable positionW = (PositionWritable) entry.get(key);
				vertex.getValue().getPositions()[key.get()] = positionW.getPosition();
			}	
		}
    	
    	System.out.println("node:: " + vertex.getId());
    	System.out.println("selectedNode broadcasted:: " + getBroadcast("selectedNodeId"));
    	boolean equal = vertex.getId().equals(getBroadcast("selectedNodeId"));
    	System.out.println("are they equal:: " + equal );
//    	
//    	RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
//    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
    	
    	IntWritable selectedNodeId = getBroadcast("selectedNodeId");

    	if (vertex.getId().equals(selectedNodeId)) {
    		System.out.println("b::: " + vertex.getValue().getB());
    		System.out.println("Length Distances:: " + vertex.getValue().getDistances().length);
    		System.out.println("PredID:: " + vertex.getValue().getPredecessorId());
    		System.out.println(":: Computing node " + vertex.getId() );
    		aggregate("selectedNodeA", vertex.getValue());
   
    		MapWritable vertexSuccessors = new MapWritable();
    		for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {  			
    			vertexSuccessors.put(edge.getTargetVertexId(), new DoubleWritable(-vertex.getValue().getDistances()[edge.getTargetVertexId().get()]));
    			vertex.removeEdges(edge.getTargetVertexId());
    		}
    		reduce("addDeleteCostForSuccessors", vertexSuccessors);
    		//ArrayWritable<Writable> messageSuccesorsId = new ArrayWritable<Writable>();
    		//messageSuccesorsId.set((Writable[]) vertexSuccessors.keySet().toArray());
    		removeEdgesRequest(new IntWritable(vertex.getValue().getPredecessorId()), vertex.getId());
    		//sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), messageSuccesorsId);
    	} else if (vertex.getValue().getPositions()[selectedNodeId.get()].equals(Position.PREDECESSOR)) {
    		/*
    		 * TODO
    		 */
    	}
    	
    	
        //sendMessageToAllEdges(vertex, new Text());
        
    }
	
	public void updatePositionsFormerSelectedNode(Vertex<IntWritable, RDCMSTValue,
			DoubleWritable> vertex, Iterable<IntWritable> messages) {
		Location bestLocation = getAggregatedValue("bestLocation");
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
		if (vertex.getId().get() == selectedNode.getId()) {
			ArrayPrimitiveWritable bestLocationPositions = getAggregatedValue("bestLocationPositions");
			vertex.getValue().setPositions((Position[]) bestLocationPositions.get());
			if (bestLocation.getWay() == Way.BREAKING_EDGE) {
				vertex.getValue().getPositions()[vertex.getId().get()] = Position.NONE;
				vertex.getValue().getPositions()[bestLocation.getNodeId()] = Position.PREDECESSOR;
			} else if (bestLocation.getWay() == Way.FROM_NODE) {
				if (messages.iterator().hasNext()) {
					IntWritable notRelatedVertexId = messages.iterator().next();
					vertex.getValue().getPositions()[notRelatedVertexId.get()] = Position.NONE;
				}
			}
		}
		
	}
}
