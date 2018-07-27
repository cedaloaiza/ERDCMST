package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

public class ElementsToComputeB implements Writable {
	
	//Id of the vertex's parent
	private int idParent;
	//Possible b value that would come from the farthest leaf of the branches that were not affected by the delete operation
	private double unaffectedBranchesB;
	//Just the distance to the only child that leads to the selected node.
	//This value plus the new b value of that child is the other possible new value of the vertex
	private double partialAffectedBranchB;
	
	public ElementsToComputeB() {
		this.idParent = -1;
		this.unaffectedBranchesB = 0;
		this.partialAffectedBranchB = 0;
	}
	
	public ElementsToComputeB(int idParent, double unaffectedBranchesB, double partialAffectedBranchB) {
		super();
		this.idParent = idParent;
		this.unaffectedBranchesB = unaffectedBranchesB;
		this.partialAffectedBranchB = partialAffectedBranchB;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(idParent);
		out.writeDouble(unaffectedBranchesB);
		out.writeDouble(partialAffectedBranchB);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		idParent = in.readInt();
		unaffectedBranchesB = in.readDouble();
		partialAffectedBranchB = in.readDouble();
		
	}
	public int getIdParent() {
		return idParent;
	}
	public double getUnaffectedBranchesB() {
		return unaffectedBranchesB;
	}
	public double getPartialAffectedBranchB() {
		return partialAffectedBranchB;
	}

}