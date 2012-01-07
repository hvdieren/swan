#!/bin/bash

THREADS=48
NUMA=$[$THREADS/8]

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    echo -n $var=$nproc $prog $flags CPU0-$[$nproc-1] mem0-$[($nproc-1)/$NUMA]" "
    eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0-$[($nproc-1)/$NUMA] ./$prog $flags > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
    if ! grep Running junk ; then echo ; fi
    rm junk
}

input=
repeat=10
#repeat=1

#prog=J_jacobi_c
#var=NONE
#for nproc in 1 ; do
#    for run in `seq $repeat` ; do
#	for flags in 64 ; do
#	    run $prog "$flags" "$input" "$var" $nproc
#	done
#    done
#done

var=NUM_THREADS
for tg in tkt cs cg ecg gtkt ; do
    prog=J_jacobi3_$tg
    for nproc in `seq 1 $THREADS` ; do
	for run in `seq $repeat` ; do
	    for flags in 64 ; do
		run $prog "$flags" "$input" $var $nproc
	    done
	done
    done
done
