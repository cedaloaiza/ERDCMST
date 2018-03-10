package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;


public class RDCMSTValue implements Writable{
	
	//The distance from this node to the faciltie
	private double f;
	//The distance from this node to the farthest leaf
	private double b;
	// Direct distance from this node to any other node in the graph .
	private double[] distances;
	// This node is; a predecessor, a successor, or none; of any node in the graph.
	private Position[] positions;
	// The Id of the unique predecessor of this node
	private int predecessorId;
	//A flag that indicates if this node can be placed in a better location in the future.
	private boolean inList;
	

	public RDCMSTValue(double f, double b, double[] distances, Position[] positions,
			int predecessorId, boolean inList) {
		this.f = f;
		this.b = b;
		this.distances = distances;
		this.positions = positions;
		this.predecessorId = predecessorId;
		this.inList = inList;
	}


	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}


	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
