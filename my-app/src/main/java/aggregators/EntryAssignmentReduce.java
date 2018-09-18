package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
		return new EntryWritable(new IntWritable(-1), new DoubleWritable(0));
		//return new EntryWritable();
	}

	@Override
	public EntryWritable reduce(EntryWritable curValue, EntryWritable valueToReduce) {
		EntryWritable response = valueToReduce;
		if (valueToReduce.getKey().equals(new IntWritable(-1))) {
			response = curValue;
		}
		return response ;
	}

	@Override
	public EntryWritable reduceMerge(EntryWritable curValue, EntryWritable valueToReduce) {
		EntryWritable response = valueToReduce;
		if (valueToReduce.getKey().equals(new IntWritable(-1))) {
			response = curValue;
		}
		return response ;
	}


}