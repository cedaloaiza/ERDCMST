package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.MapWritable;

import com.mycompany.app.RDCMSTValue;

/**
 * The map was designed to store the necessary values to update the b values of the different branches, 
 * which are born from the removing node across many superstep. 
 * However, this particular aggregator is only useful to add to the map, the initial values, 
 * which correspond to the deleting operations costs. 
 * @author cdlq1
 *
 */
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
