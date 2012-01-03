#!/bin/bash

dep=$1

run()
{
    local dep=$1
    local tasks=$2
    local batch1=$3
    local batch2=$4
    local exec=$5

    echo -ne "1 $exec $dep $tasks 1 $batch1:$batch2 0 "
    #sh -c "ulimit -t $[60*4] ;
    #       NUM_THREADS=1 numactl --physcpubind 8 --membind 1 time $exec $dep $tasks 1 $batch1:$batch2 0 > /tmp/o.$$ 2>&1"
    sh -c "ulimit -t $[60*4] ;
           NUM_THREADS=1 time $exec $dep $tasks 1 $batch1:$batch2 0 > /tmp/o.$$ 2>&1"
    a=$(egrep Per < /tmp/o.$$ | cut -d' ' -f3)
    b=$(grep system < /tmp/o.$$ | cut -d' ' -f3 | cut -de -f1)
    echo "$a $b"
    rm /tmp/o.$$
    #NUM_THREADS=1 $exec $dep $tasks 1 $batch1:$batch2 0 | grep Per | cut -d' ' -f3
}

tasks=10000000 
if [ $dep = ref -o $dep = nodep ] ; then
    tasks=100000000
fi

# fastpath
run $dep $tasks 0 0 ./data_dep1_tkt
run $dep $tasks 0 0 ./data_dep1_cs
run $dep $tasks 0 0 ./data_dep1_cg
run $dep $tasks 0 0 ./data_dep1_ecg
run $dep $tasks 0 0 ./data_dep1_vtkt
