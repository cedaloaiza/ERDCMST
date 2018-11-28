#!/bin/bash
echo "Running RDCMST on giraph..."
cd my-app
echo "Compiling..."
mvn package
echo "Removing output directory..."
rm -r ../exampleOut
echo "Executing..."
giraph target/my-app-1.0-SNAPSHOT.jar edu.icesi.app.EdgeRemovalComputation  \
-vif edu.icesi.app.RDCMSTVertexInputFormat \
-vip input/exampleRDCMST.txt \
-eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
-op ../exampleOut \
-w 1 \
-mc edu.icesi.app.RDCMSTMasterCompute \
-ca giraph.SplitMasterWorker=false \
-ca giraph.logLevel=ERROR \
-ca mapreduce.job.counters.limit=300 \
-ca mapreduce.job.counters.max=350
#-vip input/spain_locsEuclideanInput.txt \
#-vip input/exampleRDCMST.txt \

