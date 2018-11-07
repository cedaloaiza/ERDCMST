package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.MapWritable;

import edu.icesi.app.Location;

/**
 * To get best location
 * @author cesardlq
 *
 */

public class BestLocationReduce implements ReduceOperation<Location, Location> {


	@Override
	public Location createInitialValue() {
		// TODO Auto-generated method stub
		return new Location();
	}

	@Override
	public Location reduce(Location curValue, Location valueToReduce) {
		if( valueToReduce.getCost() < curValue.getCost()){
			curValue = valueToReduce;
		}
		return curValue;
	}

	@Override
	public Location reduceMerge(Location curValue, Location valueToReduce) {
		if( valueToReduce.getCost() < curValue.getCost()){
			curValue = valueToReduce;
		}
		return curValue;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
