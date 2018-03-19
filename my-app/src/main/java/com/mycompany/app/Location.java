package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
/**
 * Define a location where a node can be inserted
 * @author cesardlq
 *
 */
public class Location implements Writable {
	
	//A node and a way defines a location
	private int nodeId;
	//can be FROM NODE or BREAKING EDGE
	private Way way;
	
	//The cost of insert a node in this location
	private double cost;
	//A flag that indicates if the successors of vId will have to update their b values due to the insertion.
	private boolean arePredecessorsAffected;
	
	public Location(int nodeId, Way way, double cost, boolean arePredecessorsAffected) {
		this.nodeId = nodeId;
		this.way = way;
		this.cost = cost;
		this.arePredecessorsAffected = arePredecessorsAffected;
	}
	
	public Location() {
		this.cost = 1000;
		
	}
	
	public double getCost() {
		return cost;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nodeId);
		WritableUtils.writeEnum(out, way);
		out.writeDouble(cost);
		out.writeBoolean(arePredecessorsAffected);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId = in.readInt();
		way = WritableUtils.readEnum(in, Way.class);
		cost = in.readDouble();
		arePredecessorsAffected = in.readBoolean();
		
	}

	
}