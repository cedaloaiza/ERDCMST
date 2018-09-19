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
-vip /user/$USER/exampleRDCMST.txt \
-eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
-op /user/$USER/exampleOut \
-w 2 \
-mc edu.icesi.app.RDCMSTMasterCompute \
-ca giraph.SplitMasterWorker=true \
-ca giraph.logLevel=DEBUG \
-ca mapreduce.jobtracker.address=yarn



