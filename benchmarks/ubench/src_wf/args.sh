#!/bin/bash

dep=$1

run()
{
    local dep=$1
    local tasks=$2
    local batch1=$3
    local batch2=$4
    local exec=$5

    echo -ne "1 $exec $dep $tasks $batch1:$batch2 0 "
    sh -c "ulimit -t $[60*4] ;
           NUM_THREADS=1 numactl --physcpubind 0 --membind 0 time $exec $dep $tasks $batch1:$batch2 0 > /tmp/o.$$ 2>&1"
    a=$(egrep Per < /tmp/o.$$ | cut -d' ' -f3)
    b=$(grep system < /tmp/o.$$ | cut -d' ' -f3 | cut -de -f1)
    echo "$a $b"
    rm /tmp/o.$$
    #NUM_THREADS=1 $exec $dep $tasks $batch1:$batch2 0 | grep Per | cut -d' ' -f3
}

tasks=10000000 
if [ $dep = ref -o $dep = nodep ] ; then
    tasks=100000000
fi

vary()
{
    local dep=$1
    local tasks=$2
    local tg=$3

    echo $tg
    for args in 1 2 5 10 20 50 ; do
	run $dep $tasks 0 1000 ./data_depN${args}_$tg
    done
}

vary $dep $tasks tkt
vary $dep $tasks cs
vary $dep $tasks cg
vary $dep $tasks ecg
vary $dep $tasks gtkt
