package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.reducers.ReduceOperation;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.icesi.app.EdgeInsertionComputation;
import edu.icesi.app.RDCMSTValue;

/**

 * @author cdlq1
 *
 */
public class ArrayAssignmentReduce implements ReduceOperation<ArrayPrimitiveWritable, ArrayPrimitiveWritable> {
	
	private static final Logger LOG =
		      Logger.getLogger(ArrayAssignmentReduce.class);
	
	public ArrayAssignmentReduce(){
		
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
	public ArrayPrimitiveWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new ArrayPrimitiveWritable(new int[] {});
	}

	@Override
	public ArrayPrimitiveWritable reduce(ArrayPrimitiveWritable curValue, ArrayPrimitiveWritable valueToReduce) {
		int [] newValue = (int[]) valueToReduce.get();
		ArrayPrimitiveWritable response = valueToReduce;
		if (newValue.length == 0) {
			response = curValue;
		}
		if (LOG.isDebugEnabled()) {
          LOG.debug("in reduce:");
          LOG.debug("curValue size: " + curValue.toString());
          LOG.debug("curValue size: " + valueToReduce.toString());
		}
		return response;
	}

	@Override
	public ArrayPrimitiveWritable reduceMerge(ArrayPrimitiveWritable curValue, ArrayPrimitiveWritable valueToReduce) {
		int [] newValue = (int[]) valueToReduce.get();
		ArrayPrimitiveWritable response = valueToReduce;
		if (newValue.length == 0) {
			response = curValue;
		}
		if (LOG.isDebugEnabled()) {
          LOG.debug("in reduceMerge:");
          LOG.debug("curValue size: " + curValue.toString());
          LOG.debug("curValue size: " + valueToReduce.toString());
		}
		return response;
	}


}
