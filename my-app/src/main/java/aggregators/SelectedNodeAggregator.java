package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;

import edu.icesi.app.RDCMSTValue;

import org.apache.hadoop.io.FloatWritable;;

public class SelectedNodeAggregator extends BasicAggregator<RDCMSTValue> {

	@Override
	public void aggregate(RDCMSTValue value) {
		System.out.println("Selected Node AGGREGGATE: Aggregate node " + value.getId() + " with parent " + value.getPredecessorId() + 
				" into " + getAggregatedValue().getId() + " with parent " + getAggregatedValue().getPredecessorId());
		this.setAggregatedValue(value);
		System.out.println("After aggregate: " + getAggregatedValue().getId() + " with parent " + getAggregatedValue().getPredecessorId());
		value.print();
		
	}

	@Override
	public RDCMSTValue createInitialValue() {
//		System.out.println("Initializing SelectedNodeAggregator");
		return new RDCMSTValue();
	}

}
