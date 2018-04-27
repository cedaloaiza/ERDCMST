package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Update b and f after the delete.
   Compute the cost of inserting from node.
   Send messages to complete information

 * @author cdlq1
 *
 */
public class BFsUpdateAndBestLocationBeginningComputation extends AbstractComputation
	<IntWritable, RDCMSTValue,
	DoubleWritable, DoubleWritable, MapWritable> {

	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<DoubleWritable> messages) throws IOException {
		
		vertex.getValue().print();
		
		RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
		
		
		updateBnFs(vertex, selectedNode, messages);
		computecCostInsertingFromNode(vertex, selectedNode);
		sendBreakingEdgeInfo(vertex, selectedNode);
	}
	
	/**
	 * 
	   In this point, we have enough information to update the b and f values of every node in the graph.
	   Depending on the node, it updates its state in one way or another. 
	   If the node was a predecessor of the removing node, it has to update its b value with predecessorsDeleteCost,
	   only in the case where it does not have a different branch with a farther leaf.
	   If the node was a successor of  the removing node, it has to update its f value according to
	   the value in aggrtrAddDeleteCosts  which matches with the branch where the node is.
	   If the node is neither a predecessor nor a successor of the removing node, nothing happens. 
	 * @param vertex
	 * @param selectedNode
	 * @param message
	 */
	public void updateBnFs(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode,
			Iterable<DoubleWritable> messages){
		
		double maxPossibbleB = 0;
		for (DoubleWritable message : messages) {
			maxPossibbleB = Math.max(maxPossibbleB, message.get());
		}
	
		
	
//		IntWritable succesorId = (IntWritable) message.keySet().iterator().next();
//		DoubleWritable succesorB = (DoubleWritable) message.get(succesorId);
		DoubleWritable bestPossibleNewBDirPred = getBroadcast("bestPossibleNewBDirPred");
		
		if(vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR ){
			if(vertex.getId().equals(selectedNode.getPredecessorId())){
				if(maxPossibbleB > bestPossibleNewBDirPred.get()){
					vertex.getValue().setB(maxPossibbleB);
				}else{
					vertex.getValue().setB(bestPossibleNewBDirPred.get());
				}
			}else{
				/*
				 * TODO
				 */
			}
		}else if(vertex.getValue().getPositions()[selectedNode.getId()] == Position.SUCCESSOR ){
			MapWritable deleteCostForSuccessors = getAggregatedValue("sumDeleteCostForSuccessors");
			for(Writable branchId: deleteCostForSuccessors.keySet()) {
				IntWritable branchIdInt = (IntWritable) branchId;
				//This vertex is in branchId
				if(vertex.getValue().getPositions()[branchIdInt.get()] == Position.SUCCESSOR){
					IntWritable branchCost = (IntWritable) deleteCostForSuccessors.get(branchId);
					double newF = vertex.getValue().getF() + branchCost.get();
					vertex.getValue().setF(newF);
				}
			}
		}
	}
	
	/**
	 * compute the cost of inserting the removing node FROM NODE WAY in each node.
	 * @param vertex
	 * @param selectedNode
	 */
	public void computecCostInsertingFromNode(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode) {
		//feasible insert
		boolean feasibleInsert = (vertex.getValue().getF() + vertex.getValue().getDistances()[selectedNode.getId()] + 0) <= 10;
		if(feasibleInsert){
			double cost =  vertex.getValue().getDistances()[selectedNode.getId()];
			vertex.getValue().setPartialBestLocationCost(cost);
		}
		
	}
	
	/**
	 * send to its successor both the distance between it and them and the distance between it and the removing node.
	 * @param vertex
	 * @param selectedNode
	 */
	public void sendBreakingEdgeInfo(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode){
		for(Edge<IntWritable,DoubleWritable> edge : vertex.getEdges()){
			MapWritable messages = new MapWritable();
			IntWritable targetId = edge.getTargetVertexId();
			messages.put(new Text("TO_SUCC"), new DoubleWritable(vertex.getValue().getDistances()[targetId.get()]));
			messages.put(new Text("TO_SELEC"), new DoubleWritable(vertex.getValue().getDistances()[selectedNode.getId()]));
			messages.put(new Text("F"), new DoubleWritable(vertex.getValue().getF()));
			sendMessage(targetId, messages);
		}
	}
	
	

}