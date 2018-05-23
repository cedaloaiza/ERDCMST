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
	private int SUPER_STEPS_PER_ITERATION = 5;
	private int iteration = 0;
	private int MAX_ITERARIONS = 5;

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
		
		
		
		
		//DANGEROUS CAST!
		int superStepPhase =  (int) getSuperstep() % SUPER_STEPS_PER_ITERATION;
		System.out.println("***** Computation " +  superStepPhase + " *****");
		
		if (iteration < MAX_ITERARIONS) {		
			switch (superStepPhase) {
				//**DELETE OPERATION
				case 0:					
					//Node selection
					setComputation(EdgeRemovalComputation.class);
					Random rand = new Random();
					System.out.println(this.getClass().getName() + " - Total number of vertices: " + (int) getTotalNumVertices());
					int  selectedNodeId = rand.nextInt(3) + 1;	
					//System.out.println("Aggregator:: " + getAggregatedValue("selectedNode") );
					System.out.println("Broadcasting:: " + selectedNodeId);
					broadcast("selectedNodeId", new IntWritable(selectedNodeId));
					//
					registerReducer("addDeleteCostForSuccessors", new AddDeleteCostReduce());
					break;
				case 1:
					setComputation(EdgeInsertionComputation.class);
					MapWritable deleteCosts = getReduced("addDeleteCostForSuccessors");
					MapWritable possibleNewBsDirPred = new MapWritable();
					for (Writable dw: deleteCosts.values()) {
						System.out.println("Delete Costs:: " + dw);
						possibleNewBsDirPred.put(dw, new IntWritable(0));
					}
					setAggregatedValue("sumDeleteCostForSuccessors", deleteCosts);
					break;
				//**BEST LOCATION OPERATION
			    //For each node there are two possible ways of inserting a node:
				//1) directly as a leaf successor of the node, we called this FROM NODE WAY; and 
				//2) as a predecessor of the node, breaking the existing edge between the old predecessor and it, we called this BREAKING EDGE WAY.
				case 2:
					setComputation(BFsUpdateAndBestLocationBeginningComputation.class);
					DoubleWritable longestBranchLength = new DoubleWritable(getLongestBranchLength());
					broadcast("bestPossibleNewBDirPred", longestBranchLength);
					/**
					 * TODO
					 */
					break;
				case 3:
					computeBValues();
					setComputation(BestLocationEndingComputation.class);
					break;
				case 4:
					setComputation(insertOperationAndBFsUpdate.class);
					break;
				default:
					
					
			}
			iteration++;
		} else {
			System.out.println("Halting:: ");
			haltComputation();
		}
		
	}


	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		
		
		registerPersistentAggregator("selectedNode", SelectedNodeAggregator.class);
		//The cost which is necessary to update the values of f the successors branches of the selected node.
		//<K,V> K: Id of the one of selected node's child; V: Cost necessary to update the values of f in K branch of the selected node.
		registerPersistentAggregator("sumDeleteCostForSuccessors", SumSuccessorDeleteCostsAggregator.class);
		//New variable to decide which of the new branches, created after the removing node removal, 
		//drive now to the farthest leaf.
		registerPersistentAggregator("possibleNewBsDirPred", SumSuccessorDeleteCostsAggregator.class);
		
		registerPersistentAggregator("bestLocation", BestLocationAggregator.class);
	}
	
	/**
	 *  This value is computed as the maximum value in the possibleNewBsDirPred map, and will become
	 *  the new b value of the predecessor of the removing node,
	 *  which means that every predecessor of the removing node will have the chance of update
	 *  its own value from this value.
	 * @return
	 */
	public double getLongestBranchLength() {
		MapWritable branchLengths = getAggregatedValue("possibleNewBsDirPred");
		double largestBranchLength = 0;
		for (Writable branch: branchLengths.keySet()) {
			DoubleWritable currentLength = (DoubleWritable) branchLengths.get(branch);
			if (currentLength.get() > largestBranchLength) {
				largestBranchLength = currentLength.get();
			}
		}
		return largestBranchLength;
	}
	
	/**
	 * Compute B values for all predecessors of the selected node
	 */
	private void computeBValues() {
		// TODO Auto-generated method stub
		
	}

}
