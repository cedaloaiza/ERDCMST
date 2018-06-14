package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class RDCMSTValue implements Writable{
	
	//Id
	private int id;
	//The distance from this node to the facility
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
	//Best cost of inserting in this location in either way: FROM NODE or BREAKING EDGE
	private double partialBestLocationCost;
	




	public RDCMSTValue(double f, double b, double[] distances, Position[] positions,
			int predecessorId, boolean inList, int id) {
		this.f = f;
		this.b = b;
		this.distances = new ArrayPrimitiveWritable(distances);
		this.positions = positions;
		this.predecessorId = predecessorId;
		this.inList = inList;
		this.id = id;
	}


	public RDCMSTValue() {
		super();
		this.f = 0;
		this.b = 0;
		this.distances = new ArrayPrimitiveWritable(new double[0]);;
		this.positions = new Position[0];
		this.predecessorId = 0;
		this.inList = false;
//		this.id = 2;
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
	
	public Position[] getPositions() {
		return positions;
	}
	
	public int getId() {
		return id;
	}
	
	public void setB(double b) {
		this.b = b;
	}
	
	public double getF() {
		return f;
	}
	
	public void setF(double f) {
		this.f = f;
	}
	
	public double getPartialBestLocationCost() {
		return partialBestLocationCost;
	}


	public void setPartialBestLocationCost(double partialBestLocationCost) {
		this.partialBestLocationCost = partialBestLocationCost;
	}
	
	public void setPositions(Position[] positions) {
		this.positions = positions;
	}
	
	public void print() {
		System.out.println("***NODE " + this.id + "***");
		System.out.println("f: " + this.f);
		System.out.println("b: " + this.b);
		System.out.println("positions: " );	
		for(Position pos: positions) {
			System.out.println("\t" + pos);
		}
			
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(b);
		distances.write(out);
		out.writeInt(positions.length);
		for(Position p : positions) {
			WritableUtils.writeEnum(out, p);
		}
		out.writeInt(id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		b = in.readDouble();
		distances.readFields(in);
		int positionsLength = in.readInt();
		positions = new Position[positionsLength];
		for(int i = 0; i < positionsLength; i++) {
			positions[i] = WritableUtils.readEnum(in, Position.class);
		}
		id = in.readInt();
		
	}


}
