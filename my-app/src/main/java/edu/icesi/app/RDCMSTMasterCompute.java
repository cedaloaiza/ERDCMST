package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import aggregators.AddDeleteCostReduce;
import aggregators.BestLocationAggregator;
import aggregators.SelectedNodeAggregator;
import aggregators.SumSuccessorDeleteCostsAggregator;

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
		System.out.println("***** Computation " +  superStepPhase + " *****");
		
		switch(superStepPhase){
			//**DELETE OPERATION
			case 0:
				
				//Node selection
				setComputation(EdgeRemovalComputation.class);
				Random rand = new Random();
				//How many vertices in superstep 0?
				int  selectedNodeId = rand.nextInt(3) + 1;	
				System.out.println("Aggregator:: " + getAggregatedValue("selectedNode") );
				System.out.println("Broadcasting:: " + selectedNodeId);
				broadcast("selectedNodeId", new IntWritable(selectedNodeId));
				registerReducer("addDeleteCosts", new AddDeleteCostReduce());
				break;
			case 1:
				setComputation(EdgeInsertionComputation.class);
				MapWritable deleteCosts = getReduced("addDeleteCosts");
				for(Writable dw: deleteCosts.values()){
					System.out.println("Delete Costs:: " + dw);
				}
				setAggregatedValue("sumSuccessorsDeleteCosts", deleteCosts);
				break;
			//**BEST LOCATION OPERATION
		    //For each node there are two possible ways of inserting a node:
			//1) directly as a leaf successor of the node, we called this FROM NODE WAY; and 
			//2) as a predecessor of the node, breaking the existing edge between the old predecessor and it, we called this BREAKING EDGE WAY.
			case 2:
				setComputation(BFsUpdateAndBestLocationBeginningComputation.class);
				double longestBranchLength = getLongestBranchLength();
				RDCMSTValue selectedNode = getAggregatedValue("selectedNode");
				PredecessorsDeleteCost predecessorsDeleteCost = new PredecessorsDeleteCost(selectedNode.getPredecessorId(), longestBranchLength);
				broadcast("predecessorsDeleteCost", predecessorsDeleteCost);
				break;
			case 3:
				setComputation(BestLocationEndingComputation.class);
				break;
			case 4:
				setComputation(insertOperationAndBFsUpdate.class);
				break;
			default:
				System.out.println("Halting:: ");
				haltComputation();
				
		}
		
	}

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		
		
		registerPersistentAggregator("selectedNode", SelectedNodeAggregator.class);
		//The cost which is necessary to update the values of f the successors branches of the selected node.
		//<K,V> K: Id of the one of selected node's child; V: Cost necessary to update the values of f in K branch of the selected node.
		registerPersistentAggregator("sumSuccessorsDeleteCosts", SumSuccessorDeleteCostsAggregator.class);
		//New variable to decide which of the new branches, created after the removing node removal, 
		//drive now to the farthest leaf.
		registerPersistentAggregator("newBs", SumSuccessorDeleteCostsAggregator.class);
		
		registerPersistentAggregator("bestLocation", BestLocationAggregator.class);
	}
	
	/**
	 *  This value is computed as the maximum value in the newBs map, and will become
	 *  the new b value of the predecessor of the removing node,
	 *  which means that every predecessor of the removing node will have the chance of update
	 *  its own value from this value.
	 * @return
	 */
	public double getLongestBranchLength(){
		MapWritable branchLengths = getAggregatedValue("newBs");
		double largestBranchLength = 0;
		for(Writable branch: branchLengths.keySet()){
			DoubleWritable currentLength = (DoubleWritable) branchLengths.get(branch);
			if(currentLength.get() > largestBranchLength){
				largestBranchLength = currentLength.get();
			}
		}
		return largestBranchLength;
	}

}
