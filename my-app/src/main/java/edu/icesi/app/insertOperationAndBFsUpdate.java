package edu.icesi.app;

import java.io.IOException;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;

public class insertOperationAndBFsUpdate extends AbstractComputation
		<IntWritable, RDCMSTValue,
		DoubleWritable, IntWritable, EntryWritable> {
	
	@Override
	public void compute(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, Iterable<IntWritable> messages) throws IOException { 
		
		//vertex.getValue().print();
		
		
		Location bestLocation = getAggregatedValue("bestLocation");
		System.out.println("Best Location:: Node:" + bestLocation.getNodeId() + " Way:" + bestLocation.getWay());
		System.out.println("Real vertex ID: " + vertex.getId().get());
		RDCMSTValue selectedNode = getBroadcast("selectedNode");
		System.out.println("Positions length: " + vertex.getValue().getPositions().length);
		ArrayPrimitiveWritable selectedVertexChildrenW = getBroadcast("selectedVertexChildren");
		if (selectedVertexChildrenW != null) {
			abortMovement(vertex, selectedNode, selectedVertexChildrenW);
		} else {
		
			if (vertex.getId().get() == selectedNode.getId()) {
				if (bestLocation.getWay() == Way.FROM_NODE) {
					vertex.getValue().setPredecessorId(bestLocation.getNodeId());
				} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
					System.out.println("Connecting selected node " + selectedNode.getId() + " with best location node by breaking edge way ");
					System.out.println("Inserting edge from " + vertex.getId() + " to " + bestLocation.getNodeId());
					System.out.println("Updating parent after insert...");
					vertex.getValue().setPredecessorId(bestLocation.getPredecessorId());
					double edgeWeight = vertex.getValue().getDistances()[bestLocation.getNodeId()];
					vertex.addEdge(EdgeFactory.create(new IntWritable(bestLocation.getNodeId()), new DoubleWritable(edgeWeight)));
					aggregate("bestPossibleNewBDirPredA", new DoubleWritable(vertex.getValue().getDistances()[bestLocation.getNodeId()]));
				}
			} else if (vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.PREDECESSOR) {
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.PREDECESSOR;
				EntryWritable updatePositionMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.SUCCESSOR));
				
				//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.SUCCESSOR)});
				
				sendMessage(new IntWritable(selectedNode.getId()), updatePositionMessage);
				if (vertex.getValue().getPredecessorId() != RDCMSTValue.NONE_PARENT ) {
					EntryWritable childToInsertionMessage = new EntryWritable(new Text("ID"), vertex.getId());
					System.out.println("From best location predecessor Sending message to: " + vertex.getValue().getPredecessorId());
					sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), childToInsertionMessage);
				} 
				if (vertex.getId().get() == bestLocation.getPredecessorId() && bestLocation.getWay() == Way.BREAKING_EDGE){
					System.out.println("Inserting the selected node " + selectedNode.getId() + " from best location predecessor ");
					System.out.println("Inserting edge from " + vertex.getId() + " to " + selectedNode.getId());
					double edgeWeight = vertex.getValue().getDistances()[selectedNode.getId()];
					vertex.addEdge(EdgeFactory.create(new IntWritable(selectedNode.getId()), new DoubleWritable(edgeWeight)));
					System.out.println("Removing edge from " + vertex.getId() + " to " +  bestLocation.getNodeId());
					vertex.removeEdges(new IntWritable(bestLocation.getNodeId()));
					aggregate("bestPossibleNewBDirPredA", new DoubleWritable(vertex.getValue().getDistances()[selectedNode.getId()]));
					double selectedNodeF = vertex.getValue().getF() + vertex.getValue().getDistances()[selectedNode.getId()];
					sendMessage(new IntWritable(selectedNode.getId()), new EntryWritable(new Text("F"), new DoubleWritable(selectedNodeF)));
				}
			} else if (vertex.getValue().getPositions()[bestLocation.getNodeId()] == Position.SUCCESSOR) {
				if (bestLocation.getWay() == Way.FROM_NODE) {
					vertex.getValue().getPositions()[selectedNode.getId()] = Position.NONE;
					EntryWritable outgoingMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.NONE));
					//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.NONE)});
					sendMessage(new IntWritable(selectedNode.getId()), outgoingMessage);
					//This has to be refactored using the new type of messages
					//sendMessage(new IntWritable(selectedNode.getId()), vertex.getId());
				} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
					double newF = vertex.getValue().getF() + bestLocation.getCost();
					vertex.getValue().setF(newF);
					vertex.getValue().getPositions()[selectedNode.getId()] = Position.SUCCESSOR;
					EntryWritable outgoingMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.PREDECESSOR));
					//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.PREDECESSOR)});
					sendMessage(new IntWritable(selectedNode.getId()), outgoingMessage);
				}
			} else if (vertex.getId().get() == bestLocation.getNodeId()) {
				if (bestLocation.getWay() == Way.FROM_NODE) {
					if (bestLocation.getCost() > vertex.getValue().getB()) {
						vertex.getValue().setB(bestLocation.getCost());
					}
					//aggregate("bestLocationPositions", new ArrayPrimitiveWritable(vertex.getValue().getPositions()));
					System.out.println("Connecting best location node with selected node " + selectedNode.getId() );
					System.out.println("Inserting edge from " + vertex.getId() + " to " + selectedNode.getId());
					double edgeWeight = vertex.getValue().getDistances()[selectedNode.getId()];
					vertex.addEdge(EdgeFactory.create(new IntWritable(selectedNode.getId()), new DoubleWritable(edgeWeight)));
					vertex.getValue().getPositions()[selectedNode.getId()] = Position.PREDECESSOR;
					EntryWritable outgoingMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.SUCCESSOR));
					//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.SUCCESSOR)});
					//PositionWritable pos = (PositionWritable) outgoingMessage.get(1);
					//System.out.println("Sending from best location position: " + pos.getPosition());
					sendMessage(new IntWritable(selectedNode.getId()), outgoingMessage);
					double selectedNodeF = vertex.getValue().getF() + vertex.getValue().getDistances()[selectedNode.getId()];
					sendMessage(new IntWritable(selectedNode.getId()), new EntryWritable(new Text("F"), new DoubleWritable(selectedNodeF)));
					if (vertex.getValue().getPredecessorId() != RDCMSTValue.NONE_PARENT ) {
						EntryWritable childToInsertionMessage = new EntryWritable(new Text("ID"), vertex.getId());
						System.out.println("From best location Sending message to: " + vertex.getValue().getPredecessorId());
						sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), childToInsertionMessage);
					}
				} else if (bestLocation.getWay() == Way.BREAKING_EDGE) {
					vertex.getValue().setF(vertex.getValue().getF() + bestLocation.getCost());
					vertex.getValue().getPositions()[selectedNode.getId()] = Position.SUCCESSOR;
					System.out.println("Updating parent after insert...");
					vertex.getValue().setPredecessorId(selectedNode.getId());
					aggregate("bestPossibleNewBDirPredA", new DoubleWritable(vertex.getValue().getB()));
					sendMessage(new IntWritable(selectedNode.getId()), new EntryWritable(new Text("BEST_LOCATION_B"), new DoubleWritable(vertex.getValue().getB())));
					EntryWritable outgoingMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.PREDECESSOR));
					//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.PREDECESSOR)});
					sendMessage(new IntWritable(selectedNode.getId()), outgoingMessage);
				}
				/*
				 * TODO
				 */	
				
			} else {
				vertex.getValue().getPositions()[selectedNode.getId()] = Position.NONE;
				EntryWritable outgoingMessage = new EntryWritable(vertex.getId(), new PositionWritable(Position.NONE));
				System.out.println("From Others Sending message to: " + vertex.getValue().getPredecessorId());
				sendMessage(new IntWritable(vertex.getValue().getPredecessorId()), new EntryWritable(new Text("PARTIAL_B"), 
						new EntryWritable(vertex.getId(), new DoubleWritable(vertex.getValue().getB()))));
				//TupleWritable outgoingMessage = new TupleWritable(new Writable[] {vertex.getId(), new PositionWritable(Position.NONE)});
				sendMessage(new IntWritable(selectedNode.getId()), outgoingMessage);
			}
		}
		
		
		
		

	}

	private void abortMovement(Vertex<IntWritable, RDCMSTValue, DoubleWritable> vertex, RDCMSTValue selectedNode,
			ArrayPrimitiveWritable selectedVertexChildrenW) {
		System.out.println("Aborting movement at the insert operation");
		vertex.getValue().setB(vertex.getValue().getOldB());
		vertex.getValue().setF(vertex.getValue().getOldF());
		int[] selectedVertexChildren = (int[]) selectedVertexChildrenW.get();
		for (int child : selectedVertexChildren) {
			if (child == vertex.getValue().getId()) {
				vertex.getValue().setPredecessorId(selectedNode.getId());
			}
		}
		System.out.println("selected vertex's children: ");
		if (vertex.getId().get() == selectedNode.getId()) {
			for (int child : selectedVertexChildren) {
				System.out.println(child);
				double edgeWeight = vertex.getValue().getDistances()[child];
				vertex.addEdge(EdgeFactory.create(new IntWritable(child), new DoubleWritable(edgeWeight)));
			}
		} else if (vertex.getId().get() == selectedNode.getPredecessorId()) {
			double edgeWeight = vertex.getValue().getDistances()[selectedNode.getId()];
			vertex.addEdge(EdgeFactory.create(new IntWritable(selectedNode.getId()), new DoubleWritable(edgeWeight)));
			for (int child : selectedVertexChildren) {
				vertex.removeEdges(new IntWritable(child));
			}
		}
	}
}
