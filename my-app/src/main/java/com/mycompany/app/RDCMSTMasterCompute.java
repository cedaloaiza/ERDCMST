package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class RDCMSTMasterCompute extends MasterCompute {
	
	private ArrayList<Integer> list;

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void compute() {
		try {
			if(getSuperstep() == 0) {
				registerPersistentAggregator("selectedNode", SelectedNodeAggregator.class);
			}else{
				System.out.println("Aggregator:: " + getAggregatedValue("selectedNode") );
			}
			
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		Random rand = new Random();
		//How many vertices in superstep 0?
		int  selectedNodeId = rand.nextInt(3) + 1;
		
		broadcast("selectedNodeId", new IntWritable(selectedNodeId));
		
	}

}
