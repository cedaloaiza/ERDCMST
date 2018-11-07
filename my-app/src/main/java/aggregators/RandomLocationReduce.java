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

public class RandomLocationReduce implements ReduceOperation<Location, Location> {


	@Override
	public Location createInitialValue() {
		// TODO Auto-generated method stub
		return new Location();
	}

	@Override
	public Location reduce(Location curValue, Location valueToReduce) {
		Location selectedLocation;
		Random random = new Random();
		random.setSeed(34);
		if (random.nextBoolean()) {
			selectedLocation = valueToReduce;
			if (selectedLocation.getCost() == Double.POSITIVE_INFINITY) {
				selectedLocation = curValue;
			}
		} else {
			selectedLocation = curValue;
			if (selectedLocation.getCost() == Double.POSITIVE_INFINITY) {
				selectedLocation = valueToReduce;
			}
		}
		return selectedLocation;
	}

	@Override
	public Location reduceMerge(Location curValue, Location valueToReduce) {
		Location selectedLocation;
		Random random = new Random();
		random.setSeed(34);
		if (random.nextBoolean()) {
			selectedLocation = valueToReduce;
			if (selectedLocation.getCost() == Double.POSITIVE_INFINITY) {
				selectedLocation = curValue;
			}
		} else {
			selectedLocation = curValue;
			if (selectedLocation.getCost() == Double.POSITIVE_INFINITY) {
				selectedLocation = valueToReduce;
			}
		}
		return selectedLocation;
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
