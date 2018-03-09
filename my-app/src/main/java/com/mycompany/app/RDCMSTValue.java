package com.mycompany.app;

import java.util.HashMap;

public class RDCMSTValue {
	
	//The distance from this node to the faciltie
	private double f;
	//The distance from this node to the farthest leaf
	private double b;
	// Direct distance from this node to any other node in the graph .
	private HashMap<Integer, Double> distances;
	// This node is; a predecessor, a successor, or none; of any node in the graph.
	private HashMap<Integer, Position> positions;
	// The Id of the unique predecessor of this node
	private int predecessorId;
	//A flag that indicates if this node can be placed in a better location in the future.
	private boolean inList;
	
	

}
