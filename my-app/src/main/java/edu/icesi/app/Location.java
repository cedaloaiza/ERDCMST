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

	
	public Location(int nodeId, Way way, double cost) {
		this.nodeId = nodeId;
		this.way = way;
		this.cost = cost;
	}
	
	public Location() {
		this.nodeId = -1;
		this.cost = Double.POSITIVE_INFINITY;
		this.way = Way.FROM_NODE;
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

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nodeId);
		WritableUtils.writeEnum(out, way);
		out.writeDouble(cost);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId = in.readInt();
		way = WritableUtils.readEnum(in, Way.class);
		cost = in.readDouble();
	}

	
}
