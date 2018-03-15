package com.mycompany.app;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

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