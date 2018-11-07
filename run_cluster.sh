#!/bin/bash
FILE_INPUT=$1
if [ "$1" = "" ]; then
  FILE_INPUT="random_asymetric_1000.txt "
fi
ITERATIONS=$2
if [ "$2" = "" ]; then
  ITERATIONS=20000
fi
LAMBDA=$3
if [ "$3" = "" ]; then
  LAMBDA=1.13912
fi
INITIAL_COST=$4
if [ "$4" = "" ]; then
  INITIAL_COST=500.18758
fi
EXEC_NUM=$5
if [ "$5" = "" ]; then
  EXEC_NUM=""
fi
INPUT_FORMAT=$6
if [ "$6" = "" ]; then
  INPUT_FORMAT="edu.icesi.app.RDCMSTVertexInputFormat"
fi
THREADS=$7
if [ "$7" = "" ]; then
  THREADS=12
fi


echo "Running RDCMST on giraph..."
cd my-app
echo "Compiling..."
mvn package
echo "Removing output directory..."
hadoop dfs -rm -r exampleOut
echo "Executing..."
hadoop jar target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner edu.icesi.app.EdgeRemovalComputation \
-vif $INPUT_FORMAT \
-vip /user/$USER/$FILE_INPUT \
-eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
-op /user/$USER/exampleOut$EXEC_NUM \
-w 2 \
-mc edu.icesi.app.RDCMSTMasterCompute \
-ca giraph.SplitMasterWorker=true \
-ca giraph.logLevel=ERROR \
-ca mapreduce.jobtracker.address=yarn \
-ca giraph.numComputeThreads=$THREADS \
-ca giraph.numInputThreads=$THREADS \
-ca giraph.numOutputThreads=$THREADS \
-ca mapreduce.job.counters.max=1000 \
-ca giraph.useSuperstepCounters=false \
-ca RDCMST.maxIterations=$ITERATIONS \
-ca RDCMST.distanceConstraint=$LAMBDA \
-ca RDCMST.initialCost=$INITIAL_COST 

#-vip /user/$USER/spain_euc_complete.txt \
#-vip /user/$USER/spain_euc_333.txt \
#-vip /user/$USER/random_10.txt
#-vip /user/$USER/spain_euc_x4.txt
#-vip /user/$USER/spain_euc_x2.txt
#-vip /user/$USER/exampleRDCMST.txt
#-vip /user/$USER/spain_euc_oneAndAHalf
#-vip /user/$USER/IS_exampleRDCMST.txt 

#-vif edu.icesi.app.RDCMSTVertexInputFormat \
#-vif edu.icesi.app.ISRDCMSTVertexInputFormat \

#random_asymetric_1000.txt





