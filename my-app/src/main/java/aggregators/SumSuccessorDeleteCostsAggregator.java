package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class SumSuccessorDeleteCostsAggregator extends BasicAggregator<MapWritable> {

	@Override
	public void aggregate(MapWritable newValue) {
		
		if (newValue.size() != 0) {
			MapWritable newAggregatedValue = getAggregatedValue();
			for(Writable branchId: newValue.keySet()) {
				DoubleWritable newValueDouble = (DoubleWritable) newValue.get(branchId);
				DoubleWritable curValueDouble = (DoubleWritable) newAggregatedValue.get(branchId);
				double aggregated = newValueDouble.get();
				if( curValueDouble != null) {
					aggregated = curValueDouble.get() + newValueDouble.get(); 
				}
				newAggregatedValue.put(branchId, new DoubleWritable(aggregated));			
			}
			setAggregatedValue(newAggregatedValue);
		}
		
	}

	@Override
	public MapWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new MapWritable();
	}

}
