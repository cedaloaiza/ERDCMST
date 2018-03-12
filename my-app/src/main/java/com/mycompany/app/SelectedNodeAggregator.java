package com.mycompany.app;

import org.apache.giraph.aggregators.BasicAggregator;

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
