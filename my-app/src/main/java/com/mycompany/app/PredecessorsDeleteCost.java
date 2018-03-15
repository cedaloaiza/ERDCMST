package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * The costs which are neccesary to update predeccesors of the selected node.
 * @author cdloaiza
 *
 */
public class PredecessorsDeleteCost implements Writable {
	
	//The cost which is necessary to update the values of b in the predecessors of the selected node. 
	//Id of the selected node's father
	public int predeccesorSelectedNodeId;
	//Cost necessary to update the values of b in the predecessors of the selected node.
	public double predeccesorsCost;
	
	
	public int getPredeccesorSelectedNodeId() {
		return predeccesorSelectedNodeId;
	}

	public double getPredeccesorsCost() {
		return predeccesorsCost;
	}

	public PredecessorsDeleteCost(int predeccesorSelectedNodeId, double predeccesorsCost) {
		this.predeccesorSelectedNodeId = predeccesorSelectedNodeId;
		this.predeccesorsCost = predeccesorsCost;
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.predeccesorSelectedNodeId = in.readInt();
		this.predeccesorsCost = in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(predeccesorSelectedNodeId);
		out.writeDouble(predeccesorsCost);
		
	}

}
