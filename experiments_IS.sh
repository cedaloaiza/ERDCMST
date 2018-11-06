#!/bin/bash
#GRAPHS=("spain_euc_complete_is_repaired.txt" "spain_euc_complete_is_bkrus.txt" )
GRAPHS=("spain_euc_oneAndAHalf_is_repaired.txt" "spain_euc_oneAndAHalf_is_bkrus.txt" )
#LAMBDA=15.84738
#LAMBDA=16.158955
LAMBDA=33.79946
#ITERATIONS=9409
#ITERATIONS=18819
ITERATIONS=28228
#ITERATIONS=18

ITERS=(1)
for i in ${ITERS[@]}
# 18819 IS$i
do
  ./run_cluster.sh ${GRAPHS[i]} $ITERATIONS $LAMBDA 0 IS_oneAndAHalf$i edu.icesi.app.ISRDCMSTVertexInputFormat
  #./run_cluster.sh $GRAPH ${ITERATIONS[i]} ${LAMBDAS[i]} ${INITIAL_COSTS[i]}
done
#./run_cluster.sh spain_euc_complete.txt 120000 15.84738 72564.46 LARGE
#./run_cluster.sh spain_euc_oneAndAHalf.txt $ITERATIONS 40.200445 202952 OneAndAHalfRight
