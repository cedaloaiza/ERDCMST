package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;


public class RDCMSTValue implements Writable{
	
	//Id
	private double id;
	//The distance from this node to the faciltie
	private double f;
	//The distance from this node to the farthest leaf
	private double b;
	// Direct distance from this node to any other node in the graph .
	private ArrayPrimitiveWritable distances;
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
		this.distances = new ArrayPrimitiveWritable(distances);
		this.positions = positions;
		this.predecessorId = predecessorId;
		this.inList = inList;
	}
	
	public RDCMSTValue() {
		super();
		this.f = 0;
		this.b = 0;
		this.distances = new ArrayPrimitiveWritable(new double[1]);;
		this.positions = new Position[1];
		this.predecessorId = 0;
		this.inList = false;
	}


	
	public double[] getDistances() {
		return (double[]) distances.get();
	}
	
	public double getB() {
		return b;
	}
	
	public int getPredecessorId() {
		return predecessorId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(b);
		distances.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		b = in.readDouble();
		distances.readFields(in);
		
	}


}
