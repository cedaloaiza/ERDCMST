package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;

import edu.icesi.app.RDCMSTValue;

public class ArrayPrimitiveOverwriteAggregator extends BasicAggregator<ArrayPrimitiveWritable> {

	@Override
	public void aggregate(ArrayPrimitiveWritable value) {
		int [] newValue = (int[]) value.get();
		if (newValue.length != 0) {
			this.setAggregatedValue(value);
		}
	}

	@Override
	public ArrayPrimitiveWritable createInitialValue() {
		// TODO Auto-generated method stub
		return new ArrayPrimitiveWritable(new int[]{});
	}

}
