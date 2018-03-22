package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

public class insertOperationAndBFsUpdate extends BasicComputation
	<IntWritable, RDCMSTValue,
	DoubleWritable, MapWritable> {
	
	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<MapWritable> messages) throws IOException { 
		
		vertex.getValue().print();
		
		MapWritable receivedMessage = messages.iterator().next();
		IntWritable succesorId = (IntWritable) receivedMessage.keySet().iterator().next();
		DoubleWritable succesorB = (DoubleWritable) receivedMessage.get(succesorId);
		Location bestLocation = getAggregatedValue("bestLocation");
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
		
		if(vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.PREDECESSOR){
			double newPossibleLongestPath = succesorB.get() + bestLocation.getCost() + 
					vertex.getValue().getDistances()[succesorId.get()];
			if( newPossibleLongestPath > vertex.getValue().getB() ){				
				double newB = vertex.getValue().getB() + bestLocation.getCost();
				vertex.getValue().setB(newB);
			}
			vertex.getValue().getPositions()[selectedNode.getId()] = Position.PREDECESSOR;
		}else if(vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.SUCCESSOR){
			if(bestLocation.getWay() == Way.FROM_NODE){
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.NONE;
			}else if(bestLocation.getWay() == Way.BREAKING_EDGE){
				double newF = vertex.getValue().getF() + bestLocation.getCost();
				vertex.getValue().setF(newF);
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.SUCCESSOR;
			}
		}

	}
}
