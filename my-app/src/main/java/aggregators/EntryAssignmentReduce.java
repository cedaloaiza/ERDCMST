package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import edu.icesi.app.EntryWritable;

public class EntryAssignmentReduce implements ReduceOperation<EntryWritable, EntryWritable> {
	
	public EntryAssignmentReduce(){
		
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public EntryWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new EntryWritable();
	}

	@Override
	public EntryWritable reduce(EntryWritable curValue, EntryWritable valueToReduce) {

		return valueToReduce ;
	}

	@Override
	public EntryWritable reduceMerge(EntryWritable curValue, EntryWritable valueToReduce) {
		

		return valueToReduce;
	}


}