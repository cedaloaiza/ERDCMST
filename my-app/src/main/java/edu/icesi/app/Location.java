package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.sun.jersey.api.json.JSONConfiguration.Notation;
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
	//The id of the predecessor vertex of nodeId
	private int predecessorId;

	
	public Location(int nodeId, Way way, double cost, int predId) {
		this.nodeId = nodeId;
		this.way = way;
		this.cost = cost;
		this.predecessorId = predId;
	}
	
	public Location() {
		this.nodeId = -1;
		this.cost = Double.POSITIVE_INFINITY;
		this.way = Way.FROM_NODE;
		this.predecessorId = -1;
	}
	
	public double getCost() {
		return cost;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	
	public Way getWay() {
		return way;
	}
	
	public int getPredecessorId() {
		return predecessorId;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nodeId);
		WritableUtils.writeEnum(out, way);
		out.writeDouble(cost);
		out.writeInt(predecessorId);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId = in.readInt();
		way = WritableUtils.readEnum(in, Way.class);
		cost = in.readDouble();
		predecessorId = in.readInt();
	}

	
}
