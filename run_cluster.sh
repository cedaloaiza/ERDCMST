#!/bin/bash
echo "Running RDCMST on giraph..."
cd my-app
echo "Compiling..."
mvn package
echo "Removing output directory..."
hadoop dfs -rm -r exampleOut
echo "Executing..."
hadoop jar target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.giraph.GiraphRunner edu.icesi.app.EdgeRemovalComputation \
-vif edu.icesi.app.RDCMSTVertexInputFormat \
-vip /user/$USER/spain_euc_complete.txt \
-eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
-op /user/$USER/exampleOut \
-w 14 \
-mc edu.icesi.app.RDCMSTMasterCompute \
-ca giraph.SplitMasterWorker=true \
-ca giraph.logLevel=ERROR \
-ca mapreduce.jobtracker.address=yarn \
-ca giraph.numComputeThreads=4 \
-ca giraph.numInputThreads=4 \
-ca giraph.numOutputThreads=4 \
-ca mapreduce.job.counters.max=1000 \
-ca giraph.useSuperstepCounters=false 

#-vip /user/$USER/spain_euc_complete.txt \
#-vip /user/$USER/spain_euc_333.txt \
#-vip /user/$USER/random_10.txt
#-vip /user/$USER/spain_euc_x4.txt
#-vip /user/$USER/spain_euc_x2.txt



