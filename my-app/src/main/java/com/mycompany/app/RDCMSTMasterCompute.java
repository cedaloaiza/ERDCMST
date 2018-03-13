package com.mycompany.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import aggregators.AddDeleteCostReduce;
import aggregators.SelectedNodeAggregator;

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
		
		
		
		int superStepsPerIteration = 5;
		//DANGEROUS CAST!
		int superStepPhase =  (int)getSuperstep() % superStepsPerIteration;
		System.out.println("Master Compute phase:: " + superStepPhase);
		
		switch(superStepPhase){
			case 0:
				setComputation(EdgeRemovalCompute.class);
				Random rand = new Random();
				//How many vertices in superstep 0?
				int  selectedNodeId = rand.nextInt(3) + 1;
				System.out.println("Broadcasting:: " + selectedNodeId);
				System.out.println("Aggregator:: " + getAggregatedValue("selectedNode") );
				broadcast("selectedNodeId", new IntWritable(selectedNodeId));
				registerReducer("addDeleteCosts", new AddDeleteCostReduce());
				break;
			case 1:
				//setComputation(EdgeInsertionCompute.class);
				MapWritable deleteCosts = getReduced("addDeleteCosts");
				for(Writable dw: deleteCosts.values()){
					System.out.println("Delete Costs:: " + dw);
				}
				System.out.println("Halting:: ");
				haltComputation();
				break;
			default:
				;
				
		}
		
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		
		
		registerPersistentAggregator("selectedNode", SelectedNodeAggregator.class);
		
		
	}

}
