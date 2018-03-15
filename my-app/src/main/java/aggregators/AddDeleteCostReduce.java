package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.MapWritable;

import com.mycompany.app.RDCMSTValue;

public class AddDeleteCostReduce implements ReduceOperation<MapWritable, MapWritable> {
	
	public AddDeleteCostReduce(){
		
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
	public MapWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new MapWritable();
	}

	@Override
	public MapWritable reduce(MapWritable curValue, MapWritable valueToReduce) {
		// TODO Auto-generated method stub
		return valueToReduce ;
	}

	@Override
	public MapWritable reduceMerge(MapWritable curValue, MapWritable valueToReduce) {
		// TODO Auto-generated method stub
		return valueToReduce;
	}


}
