#!/bin/bash
GRAPHS=("spain_euc_complete.txt" "spain_euc_half.txt" "spain_euc_oneAndAHalf.txt")
#GRAPHS=("spain_euc_complete.txt" "spain_euc_half.txt")
ITERATIONS=(18819 9409 28228)
#ITERATIONS=(18819 9409)
#ITERATIONS=(10 20 30)
LAMBDAS=(15.84738 16.158955 33.79946)
LAMBDAS_COMPLETE=(15.84738 11.83869 19.85607)
LAMBDAS_HALF=(16.158955 11.9944775 20.3234325)
LAMBDAS_ONENAHALF=(33.79946 27.398475 40.200445)
INITIAL_COSTS=(72564.46 36219.45 202952)
#THREADS=(4 12)
ITERS=(0 1 2)
for i in ${ITERS[@]}
do
  ./run_cluster.sh ${GRAPHS[i]} ${ITERATIONS[i]} ${LAMBDAS[i]} ${INITIAL_COSTS[i]} _improvements_$i

  #./run_cluster.sh $GRAPH ${ITERATIONS[i]} ${LAMBDAS[i]} ${INITIAL_COSTS[i]}
done
