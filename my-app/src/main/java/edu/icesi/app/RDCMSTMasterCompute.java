package edu.icesi.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import aggregators.MapAssignmentReduce;
import aggregators.BestLocationAggregator;
import aggregators.EntryAssignmentReduce;
import aggregators.ArrayPrimitiveOverwriteAggregator;
import aggregators.SelectedNodeAggregator;
import aggregators.SumSuccessorDeleteCostsAggregator;

public class RDCMSTMasterCompute extends MasterCompute {
	
	private ArrayList<Integer> list;
	private int SUPER_STEPS_PER_ITERATION = 5;
	private int iteration = 0;
	private int MAX_ITERARIONS = 6;
	private int lambda = 100;
	private int superstepDeviation = 0;
	
	//JUST FOR DEBUGGING
	private int[] selectedNodes = new int[]{2, 1, 3, 2, 3, 2};
	
	private RDCMSTValue selectedNode;

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
		int superStepPhase =  ((int) getSuperstep() + superstepDeviation) % SUPER_STEPS_PER_ITERATION;
		System.out.println("\n\n" + iteration);
		System.out.println("Iteration/Movement: " + iteration);
		System.out.println("***** Computation " +  superStepPhase + " *****");
		System.out.println("Number of vertices: " + getTotalNumVertices());
		System.out.println("***MASTER ***");
		if (iteration < MAX_ITERARIONS) {		
			switch (superStepPhase) {
				//**DELETE OPERATION
				case 0:					
					selectANode();
					break;
				case 1:
					computeBValues();
					setAggregatedValue("bestLocation", new Location());
					selectedNode = getAggregatedValue("selectedNodeA");
					System.out.println("Selected node's parent at master: " + selectedNode.getPredecessorId());
					broadcast("selectedNode", selectedNode);
					setComputation(EdgeInsertionComputation.class);
					MapWritable deleteCosts = getReduced("addDeleteCostForSuccessors");
					System.out.println("Length of KeySet of reduce operation: " + deleteCosts.keySet().size());
					MapWritable possibleNewBsDirPred = new MapWritable();
					for (Writable dw: deleteCosts.keySet()) {
						System.out.println("Key: " + dw + " - Delete Costs:: " + deleteCosts.get(dw));
						possibleNewBsDirPred.put(dw, new IntWritable(0));
					}
					setAggregatedValue("sumDeleteCostForSuccessors", deleteCosts);
					break;
				//**BEST LOCATION OPERATION
			    //For each node there are two possible ways of inserting a node:
				//1) directly as a leaf successor of the node, we called this FROM NODE WAY; and 
				//2) as a predecessor of the node, breaking the existing edge between the old predecessor and it, we called this BREAKING EDGE WAY.
				case 2:
//					selectedNode = getAggregatedValue("selectedNode");
//					Location bestLocation = getAggregatedValue("bestLocation");
//					System.out.println("Selected node at master Compute 2: " + selectedNode.getId());
//					System.out.println("Best Location at master Compute 2: " + bestLocation.getNodeId());
					
					double bestPossibleNewBDirPred = getLongestBranchLength();
					DoubleWritable parentF = (DoubleWritable) getAggregatedValue("parentF");
					if (parentF.get() + bestPossibleNewBDirPred > lambda) {
						superstepDeviation += 3;
						broadcast("everythingUpdated", new BooleanWritable(true));
						selectANode();
						iteration++;
						break;
					}
					broadcast("selectedNode", selectedNode);
					registerReducer("parentB", new EntryAssignmentReduce());
					registerReducer("allPredecessorsPossibleNewBs", new MapAssignmentReduce());
					setComputation(BFsUpdateAndBestLocationBeginningComputation.class);
					DoubleWritable longestBranchLength = new DoubleWritable(getLongestBranchLength());
					broadcast("bestPossibleNewBDirPred", longestBranchLength);
					/**
					 * TODO
					 */
					break;
				case 3:
					broadcast("selectedNode", selectedNode);
					computeBValues();
					setComputation(BestLocationEndingComputation.class);
					break;
				case 4:
					Location bl = getAggregatedValue("bestLocation");
					System.out.println("Best Location: ");
					bl.print();
					broadcast("selectedNode", selectedNode);
					setComputation(insertOperationAndBFsUpdate.class);
					iteration++;
					break;
				default:
					
					
			}
		} else {
			System.out.println("Halting:: ");
			haltComputation();
		}
		
	}


	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {

		System.out.println("Master compute's initialize()");
		
		selectedNode = new RDCMSTValue();
		selectedNode.setId(-1);
		
		registerAggregator("selectedNodeA", SelectedNodeAggregator.class);
		//The cost which is necessary to update the values of f the successors branches of the selected node.
		//<K,V> K: Id of the one of selected node's child; V: Cost necessary to update the values of f in K branch of the selected node.
		registerPersistentAggregator("sumDeleteCostForSuccessors", SumSuccessorDeleteCostsAggregator.class);
		//New variable to decide which of the new branches, created after the removing node removal, 
		//drive now to the farthest leaf.
		registerAggregator("possibleNewBsDirPred", SumSuccessorDeleteCostsAggregator.class);
		
		registerPersistentAggregator("bestLocation", BestLocationAggregator.class);
		
		registerAggregator("parentF", DoubleSumAggregator.class);
		
		registerAggregator("bestPossibleNewBDirPredA", DoubleSumAggregator.class);
		
		
		
		//We are doing the positions' update of selected node just with messages
//		registerPersistentAggregator("bestLocationPositions", ArrayPrimitiveOverwriteAggregator.class);
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
		MapWritable allPredecessorsPossibleNewBs = getReduced("allPredecessorsPossibleNewBs");
		MapWritable newBs = new MapWritable();
		EntryWritable parentB = getReduced("parentB");
		IntWritable parentId = (IntWritable) parentB.getKey();
		DoubleWritable bValueWritable = (DoubleWritable) parentB.get(parentId);
		double bValue = bValueWritable.get();
		while (!allPredecessorsPossibleNewBs.isEmpty()) {
			System.out.println("allPredecessorsPossibleNewBs length: " + allPredecessorsPossibleNewBs.size());
			ElementsToComputeB elementsToComputeB = (ElementsToComputeB) allPredecessorsPossibleNewBs.get(parentId);
			double unaffectedBranchesB = elementsToComputeB.getUnaffectedBranchesB();
			double affectedBranchB = elementsToComputeB.getPartialAffectedBranchB() +  bValue;
			if (unaffectedBranchesB > affectedBranchB) {
				bValue = unaffectedBranchesB;
			} else {
				bValue = affectedBranchB;
			}
			System.out.println("Storing new b value for " + parentId + ": " + bValue);
			newBs.put(parentId, new DoubleWritable(bValue));
			allPredecessorsPossibleNewBs.remove(parentId);
			//WARNING!!!!!
			parentId = new IntWritable(elementsToComputeB.getIdParent());
			System.out.println("Parent ID " + parentId);
		}
		broadcast("newBs", newBs);
		
	}
	
	private void selectANode() {
		//Node selection
		setComputation(EdgeRemovalComputation.class);
		Random rand = new Random();
		System.out.println(this.getClass().getName() + " - Total number of vertices: " + (int) getTotalNumVertices());
//		int  selectedNodeId = rand.nextInt(3) + 1;	
		//JUST FOE DEBUGGING
		int  selectedNodeId = selectedNodes[iteration];
		//System.out.println("Aggregator:: " + getAggregatedValue("selectedNode") );
		System.out.println("Broadcasting:: " + selectedNodeId);
		broadcast("selectedNodeId", new IntWritable(selectedNodeId));
		broadcast("selectedNode", selectedNode);
		//
		registerReducer("addDeleteCostForSuccessors", new MapAssignmentReduce());
		registerReducer("parentB", new EntryAssignmentReduce(), new EntryWritable());
		registerReducer("allPredecessorsPossibleNewBs", new MapAssignmentReduce());
	}
	
	private void resetPersistentAggregators() {
		setAggregatedValue("sumDeleteCostForSuccessors", new MapWritable());
		setAggregatedValue("sumDeleteCostForSuccessors", new MapWritable());
	}
	

}
