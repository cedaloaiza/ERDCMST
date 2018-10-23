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
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import aggregators.MapAssignmentReduce;
import aggregators.BestLocationAggregator;
import aggregators.EntryAssignmentReduce;
import aggregators.ArrayAssignmentReduce;
import aggregators.ArrayPrimitiveOverwriteAggregator;
import aggregators.SelectedNodeAggregator;
import aggregators.SumSuccessorDeleteCostsAggregator;

public class RDCMSTMasterCompute extends MasterCompute {
	
	private ArrayList<Integer> list;
	private int SUPER_STEPS_PER_ITERATION = 5;
	private int iteration = 0;
	//JUST FOR DEBUGGING
	//private int[] selectedNodes = new int[]{2, 3, 1, 4, 1, 3};
	private int[] selectedNodes = new int[]{2, 10, 9, 4, 1, 3, 2, 1, 8, 7, 3, 1, 9, 6, 5, 10, 9, 6, 2, 1, 3, 4, 6, 7, 8, 9};
	private int MAX_ITERARIONS = 18819;
	//private int MAX_ITERARIONS = selectedNodes.length;
	private int lambda = 1000000;
	private int superstepDeviation = 0;
	private ArrayPrimitiveWritable selectedVertexChildrenWritable;
	//Aborted movement because cost is not improved
	private boolean abortedMovement = false;
	private boolean feasibleDelete = true;
	private Random rand;
	private ArrayList<Integer> pendingVertices;
	private ArrayList<Integer> selectedVertices;
	
	
	
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
					if (!abortedMovement) {
						broadcast("startingNormalMovement", new BooleanWritable(true));
					}
					selectANode();
					break;
				case 1:
					//Completing former movement
					computeBValues();
					setAggregatedValue("bestLocation", new Location());
					abortedMovement = false;
					
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
						selectANode();
						iteration++;
						feasibleDelete = false;
						break;
					}
					feasibleDelete = true;
					broadcast("selectedNode", selectedNode);
					registerReducer("parentB", new EntryAssignmentReduce());
					registerReducer("allPredecessorsPossibleNewBs", new MapAssignmentReduce());
					setComputation(BFsUpdateAndBestLocationBeginningComputation.class);
					DoubleWritable longestBranchLength = new DoubleWritable(bestPossibleNewBDirPred);
					broadcast("bestPossibleNewBDirPred", longestBranchLength);
					registerReducer("selectedVertexChildren", new ArrayAssignmentReduce());
					/**
					 * TODO
					 */
					break;
				case 3:
					selectedVertexChildrenWritable = getReduced("selectedVertexChildren");
					broadcast("selectedNode", selectedNode);
					computeBValues();
					setComputation(BestLocationEndingComputation.class);
					break;
				case 4:
					Location bl = getAggregatedValue("bestLocation");
					System.out.println("Best Location: ");
					bl.print();
					DoubleWritable movementCostW = getAggregatedValue("movementCost");
					double movementCost = movementCostW.get() + bl.getCost();
					System.out.println("Movement Cost: " +  movementCost);
					if (movementCost >= 0) {
						broadcast("selectedVertexChildren", selectedVertexChildrenWritable);
						abortedMovement = true;
					} else {
						abortedMovement = false;
						if (pendingVertices != null) {
							System.out.println("An non-aborted movement!!!");
							pendingVertices.addAll(selectedVertices);
							selectedVertices = new ArrayList<Integer>();
						}
					}
					broadcast("selectedNode", selectedNode);
					setComputation(insertOperationAndBFsUpdate.class);
					setAggregatedValue("movementCost", new DoubleWritable(0));
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
		
		registerPersistentAggregator("movementCost", DoubleSumAggregator.class);
		
		registerAggregator("parentF", DoubleSumAggregator.class);
		
		registerAggregator("bestPossibleNewBDirPredA", DoubleSumAggregator.class);
		
		
		rand = new Random();
		rand.setSeed(34);
		
		selectedVertices = new ArrayList<Integer>();
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
		System.out.println("Possible bs from unaffected branch for selected node's parent:");
		for (Writable branch: branchLengths.keySet()) {
			DoubleWritable currentLength = (DoubleWritable) branchLengths.get(branch);
			System.out.println("branch: " + branch.toString() + " b: " +  branchLengths.get(branch).toString());
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
		System.out.println("Computing new b values...");
		MapWritable allPredecessorsPossibleNewBs = getReduced("allPredecessorsPossibleNewBs");
		MapWritable newBs = new MapWritable();
		EntryWritable parentB = getReduced("parentB");
		IntWritable parentId = (IntWritable) parentB.getKey();
		DoubleWritable bValueWritable = (DoubleWritable) parentB.get(parentId);
		System.out.println("Are different?: " + (int) getSuperstep() + " of " + 1 + ": " + ((int) getSuperstep() != 1));
		if ((int) getSuperstep() != 1 && !abortedMovement && feasibleDelete) {
			System.out.println("parent of selected node: " + parentId.get());
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
		}
		broadcast("newBs", newBs);
		
	}
	
	private void selectANode() {
		//Node selection
		setComputation(EdgeRemovalComputation.class);
		System.out.println(this.getClass().getName() + " - Total number of vertices: " + (int) getTotalNumVertices());
		int  selectedNodeId;
		if (getSuperstep() == 0) {
			selectedNodeId = 1;
		} else {
			if (pendingVertices == null) {
				pendingVertices = new ArrayList<Integer>();
				System.out.println("Initializing pending vertices...");
				for (int i = 1; i < (int) getTotalNumVertices(); i++) {
					pendingVertices.add(i);
				}
			}
			System.out.println("Pending vertices size: " + pendingVertices.size());	
			if (pendingVertices.isEmpty()) {
				System.out.println("Local minimum found!");
				System.out.println("!!!!Hecatombe!!!!!");
				haltComputation();
				return;
			} 
			int selectedNodeIndex = rand.nextInt(pendingVertices.size());
			//selectedNodeId = rand.nextInt((int) getTotalNumVertices() - 1) + 1;
			System.out.println("Index selected vertex: " + selectedNodeIndex);
			selectedNodeId = pendingVertices.remove(selectedNodeIndex);
			selectedVertices.add(selectedNodeId);
		}
		//JUST FOE DEBUGGING
//		int  selectedNodeId = selectedNodes[iteration];
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
