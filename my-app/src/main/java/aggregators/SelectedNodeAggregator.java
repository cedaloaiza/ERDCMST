package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

import com.mycompany.app.RDCMSTValue;

import org.apache.hadoop.io.FloatWritable;;

public class SelectedNodeAggregator extends BasicAggregator<RDCMSTValue> {

	@Override
	public void aggregate(RDCMSTValue value) {
		this.setAggregatedValue(value);
		
	}

	@Override
	public RDCMSTValue createInitialValue() {
		// TODO Auto-generated method stub
		return new RDCMSTValue();
	}

}
