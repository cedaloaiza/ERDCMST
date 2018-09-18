package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import edu.icesi.app.RDCMSTValue;

/**
 * The value of this ReduceOperation was designed to store the necessary values to update the f values of the different branches that 
 * are born from the selected node after the Delete Operation. 
 * However, this particular ReduceOperation is only useful to add to the map, the initial values, 
 * which correspond to the costs of the edge removal phase. Later, the costs of the edge insertion phase will be added and we will consolidate the costs of 
 * the complete Delete Operation. 
 * @author cdlq1
 *
 */
public class MapAssignmentReduce implements ReduceOperation<MapWritable, MapWritable> {
	
	public MapAssignmentReduce(){
		
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
		if (valueToReduce.size() != 0) {
			System.out.println("*reduce*");
			System.out.println("reducing map of size " + valueToReduce.size() + " into a map of size " + curValue.size());
			for (Writable dw: valueToReduce.keySet()) {
				System.out.println("Key: " + dw + " - Delete Costs:: " + valueToReduce.get(dw));
			}
			curValue.putAll(valueToReduce);
		}
		return  curValue;
	}

	@Override
	public MapWritable reduceMerge(MapWritable curValue, MapWritable valueToReduce) {
		if (valueToReduce.size() != 0) {
			System.out.println("*reduceMerge*");
			System.out.println("reducing map of size " + valueToReduce.size() + " into a map of size " + curValue.size());
			for (Writable dw: valueToReduce.keySet()) {
				System.out.println("Key: " + dw + " - Delete Costs:: " + valueToReduce.get(dw));
			}
			curValue.putAll(valueToReduce);
		}
		return curValue;
	}


}
