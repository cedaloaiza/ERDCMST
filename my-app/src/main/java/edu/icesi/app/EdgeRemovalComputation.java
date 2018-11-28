package edu.icesi.app;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.Iterables;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
        DoubleWritable, EntryWritable, DoubleWritable> {
	
	private static final Logger LOG =
		      Logger.getLogger(EdgeRemovalComputation.class);
   
	public void compute(Vertex<IntWritable, RDCMSTValue,
			DoubleWritable> vertex, Iterable<EntryWritable> messages) throws IOException {
		
		
		IntWritable localMinimumCounter = getBroadcast("localMinimum");
		if (localMinimumCounter != null) {
		    for (Edge<IntWritable, DoubleWritable> e : vertex.getEdges()) {
		    	System.out.println(vertex.getId().get() + " " + e.getTargetVertexId().get() + " " + e.getValue());
		    }
		}
		
		BooleanWritable startingNormalMovement = getBroadcast("startingNormalMovement");
		if (startingNormalMovement != null && (int) getSuperstep() != 0) {
			RDCMSTValue selectedNode = getBroadcast("selectedNode");
			if (LOG.isDebugEnabled()) {
  	          LOG.debug("Old selected vertex: " + selectedNode.getId());
			}
			//Location bestLocation = getAggregatedValue("bestLocation");
			Location bestLocation = getBroadcast("bestLocationAggregated");
			completePreviousMovement(vertex, messages, selectedNode, bestLocation);
		}
		
		/*
		if (LOG.isDebugEnabled()) {
			vertex.getValue().print();
		}
		*/
		/*
		System.out.print("Children: ");
		for (Edge<IntWritable, DoubleWritable> e : vertex.getEdges()) {
			System.out.print(e.getTargetVertexId() + " ");
		}
		*/
		//System.out.println("");
		//Completing the previous movement
		
		
		//JUST FOR DEBUGGING
//		Location bestLocation = getAggregatedValue("bestLocation");
//		System.out.println("Best Location at superstep 0:: Node:" + bestLocation.getNodeId() + " Way:" + bestLocation.getWay());
		
    	
		//ERASE
//    	System.out.println("node:: " + vertex.getId());
//    	System.out.println("selectedNode broadcasted:: " + getBroadcast("selectedNodeId"));
//    	boolean equal = vertex.getId().equals(getBroadcast("selectedNodeId"));
//    	System.out.println("are they equal:: " + equal );
//    	
//    	RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
//    	IntWritable selectedNodeId = new IntWritable(selectedNode.getId());
    	
    	IntWritable selectedNodeId = getBroadcast("selectedNodeId");

    	if (vertex.getId().equals(selectedNodeId)) {
    		
    		//ERASE
//    		System.out.println("b::: " + vertex.getValue().getB());
//    		System.out.println("Length Distances:: " + vertex.getValue().getDistances().length);
//    		System.out.println("PredID:: " + vertex.getValue().getPredecessorId());
//    		System.out.println(":: Computing node " + vertex.getId() );
    		if (LOG.isDebugEnabled()) {
    	          LOG.debug("Selected node's parent before aggregate: " + vertex.getValue().getPredecessorId());
    		}
    		aggregate("selectedNodeA", vertex.getValue());
   
    		MapWritable vertexSuccessors = new MapWritable();
    		
    		double movementCost = 0;
    		
    		for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {
    			if (LOG.isDebugEnabled()) {
      	          LOG.debug("Key: " + edge.getTargetVertexId() + " - Delete Costs:: " + -vertex.getValue().getDistances()[edge.getTargetVertexId().get()]);
    			}
    			vertexSuccessors.put(new IntWritable(edge.getTargetVertexId().get()), new DoubleWritable(-vertex.getValue().getDistances()[edge.getTargetVertexId().get()]));
    			movementCost -= vertex.getValue().getDistances()[edge.getTargetVertexId().get()];
    		}
    		if (LOG.isDebugEnabled()) {
    	          LOG.debug("Size of reduced object: " + vertexSuccessors.size());
    	          for (Writable dw: vertexSuccessors.keySet()) {
    	        	  LOG.debug("Key: " + dw + " - Delete Costs:: " + vertexSuccessors.get(dw));
		    		}
  			}
    		
    		reduce("addDeleteCostForSuccessors", vertexSuccessors);
    		aggregate("movementCost", new DoubleWritable(movementCost));
    		//ArrayWritable<Writable> messageSuccesorsId = new ArrayWritable<Writable>();
    		//messageSuccesorsId.set((Writable[]) vertexSuccessors.keySet().toArray());
    		//sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), messageSuccesorsId);
    	} else if (vertex.getValue().getPositions()[selectedNodeId.get()].equals(Position.PREDECESSOR)) {
    		for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {  		
    			double distanceToChild = vertex.getValue().getDistances()[edge.getTargetVertexId().get()];
    			if (LOG.isDebugEnabled()) {
      	          LOG.debug("Distance to child: " + distanceToChild);
    			}
    			sendMessage(edge.getTargetVertexId(), new DoubleWritable(distanceToChild));
    		}
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
	
	public void completePreviousMovement (Vertex<IntWritable, RDCMSTValue,
			DoubleWritable> vertex, Iterable<EntryWritable> messages, RDCMSTValue selectedNode, Location bestLocation) {
		
		if (LOG.isDebugEnabled()) {
          LOG.debug("Positions length: " + vertex.getValue().getPositions().length);
          LOG.debug("Is selected node´s predecessor? " + (vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR));
		}
		if (selectedNode.getId() == vertex.getId().get()) {
			double selectedNodeNewF = 0;
			double bestLocationB = 0;
			for (EntryWritable entry : messages) {
				if (entry.getKey() instanceof Text) {
					Text header = (Text) entry.getKey();
					if (header.toString().equals("F")) {
						DoubleWritable selectedNodeNewFW = (DoubleWritable) entry.get(header);
						selectedNodeNewF = selectedNodeNewFW.get();
					} else if (header.toString().equals("BEST_LOCATION_B")) {
						DoubleWritable bestLocationBW = (DoubleWritable) entry.get(header);
						bestLocationB = bestLocationBW.get();
					}
				} else {
					IntWritable key = (IntWritable) entry.getKey();
					PositionWritable positionW = (PositionWritable) entry.get(key);
					vertex.getValue().getPositions()[key.get()] = positionW.getPosition();
					if (LOG.isDebugEnabled()) {
	      	          LOG.debug("Updating positions of selected node");
	      	          LOG.debug("Key: " + key);
	      	          LOG.debug("Position: " + positionW.getPosition());
	      	          LOG.debug("Updating positions of selected node");
	    			}
				}
			}
			vertex.getValue().setF(selectedNodeNewF);
			if (bestLocation.getWay() == Way.FROM_NODE) {
				vertex.getValue().setB(0);
			} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
				double newB = bestLocationB + vertex.getValue().getDistances()[bestLocation.getNodeId()]; 
				vertex.getValue().setB(newB);
			}
		} else if (vertex.getValue().getPositions()[selectedNode.getId()] == Position.PREDECESSOR) {
			DoubleWritable bestPossibleNewBDirPred = getAggregatedValue("bestPossibleNewBDirPredA");
			int childToSelectedNode = -1;
			double maxPossibbleB = 0;
			if (LOG.isDebugEnabled()) {
	          LOG.debug("Computing b from unaffected branches...");
  			}
			for (EntryWritable message : messages) {
				if (LOG.isDebugEnabled()) {
      	          LOG.debug("maxPossibbleB: " + maxPossibbleB );
    			}
				Text messageKey = (Text) message.getKey();
				if (messageKey.toString().equals("PARTIAL_B")) {
					EntryWritable entryPartialB = (EntryWritable) message.get(message.getKey());
					IntWritable emisorId = (IntWritable) entryPartialB.getKey();
					DoubleWritable partialPossibleB = (DoubleWritable)entryPartialB.get(entryPartialB.getKey()); 
					double possibleB = partialPossibleB.get() + vertex.getValue().getDistances()[emisorId.get()];
					maxPossibbleB = Math.max(maxPossibbleB,  possibleB );
				} else if (messageKey.toString().equals("ID")) {
					IntWritable childToSelectedNodeWritable = (IntWritable) message.get(messageKey);
					childToSelectedNode = childToSelectedNodeWritable.get();
				}
			}
			if (LOG.isDebugEnabled()) {
				bestLocation.print();
			}
			//if vertex is parent of selected node
			if (vertex.getId().get() == bestLocation.getPredecessorId() && bestLocation.getWay() == Way.BREAKING_EDGE || 
					vertex.getId().get() == bestLocation.getNodeId() && bestLocation.getWay() == Way.FROM_NODE) {
				if (bestLocation.getWay() == Way.BREAKING_EDGE) {
					if (maxPossibbleB > bestPossibleNewBDirPred.get()) {
						vertex.getValue().setB(maxPossibbleB);
					} else {
						vertex.getValue().setB(bestPossibleNewBDirPred.get());
					}
				}
				if (LOG.isDebugEnabled()) {
      	          LOG.debug("b value of parent vertex " + vertex.getId() + ": " + vertex.getValue().getB());
    			}
				reduce("parentB", new EntryWritable(new IntWritable(vertex.getValue().getPredecessorId()), new DoubleWritable(vertex.getValue().getB())));
			} else {
				ElementsToComputeB elementsToComputeB = new ElementsToComputeB(vertex.getValue().getPredecessorId(), maxPossibbleB, 
						vertex.getValue().getDistances()[childToSelectedNode]);			
				MapWritable elementsToComputeBMap = new MapWritable();
				elementsToComputeBMap.put(vertex.getId(), elementsToComputeB);
				reduce("allPredecessorsPossibleNewBs", elementsToComputeBMap);
			}
		}
	}
}
