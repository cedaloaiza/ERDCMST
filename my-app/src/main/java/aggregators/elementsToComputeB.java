package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Writable;

import edu.icesi.app.Position;

public class elementsToComputeB implements Writable{
	
	public static final int NONE_PARENT = -1;
	
	//Id of the vertex's parent
	private int idParent;
	//The distance from this node to the facility
	private double unaffectedBranchesB;
	//The distance from this node to the farthest leaf
	private double partialAffectedBranchB;
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(idParent);
		out.writeDouble(unaffectedBranchesB);
		out.writeDouble(partialAffectedBranchB);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

}