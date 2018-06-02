package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

public class insertOperationAndBFsUpdate extends BasicComputation
	<IntWritable, RDCMSTValue,
	DoubleWritable, IntWritable> {
	
	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<IntWritable> messages) throws IOException { 
		
		vertex.getValue().print();
		
		
		Location bestLocation = getAggregatedValue("bestLocation");
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
		
		if (vertex.getId().get() == selectedNode.getId()) {
			if (bestLocation.getWay() == Way.FROM_NODE) {
				/*
				 * TODO 
				 */
			} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
				/*
				 * TODO
				 */
				vertex.addEdge(EdgeFactory.create(new IntWritable(bestLocation.getNodeId()), new DoubleWritable(5)));
			}
		} else if (vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.PREDECESSOR) {
			if (messages.iterator().hasNext()) {
				IntWritable succesorId = messages.iterator().next();
				/*
				 * TODO
				 */
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.PREDECESSOR;
			}
			if (vertex.getId().get() == bestLocation.getPredecessorId()){
				vertex.addEdge(EdgeFactory.create(new IntWritable(selectedNode.getId()), new DoubleWritable(5)));
				vertex.removeEdges(new IntWritable(bestLocation.getNodeId()));
			}
		} else if (vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.SUCCESSOR) {
			if (bestLocation.getWay() == Way.FROM_NODE) {
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.NONE;
			} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
				double newF = vertex.getValue().getF() + bestLocation.getCost();
				vertex.getValue().setF(newF);
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.SUCCESSOR;
				selectedNode.getPositions()[vertex.getId().get()] = Position.PREDECESSOR;
			}
		} else if (vertex.getId().get() == bestLocation.getNodeId() && bestLocation.getWay() == Way.FROM_NODE) {
			/*
			 * TODO
			 */
			vertex.addEdge(EdgeFactory.create(new IntWritable(selectedNode.getId()), new DoubleWritable(5)));
		}
		
		
		
		

	}
}
