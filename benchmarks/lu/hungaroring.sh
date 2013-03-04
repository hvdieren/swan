#!/bin/bash

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    echo -n $var=$nproc $prog $input CPU0-$[$nproc-1] mem0-$[($nproc-1)/8]" "
    eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0-$[($nproc-1)/8] ./$prog $input > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
    if ! grep Running junk ; then echo ; fi
    rm junk
}

repeat=10
input="-n 4096"
flags=

prog=lu_c
var=NONE
for nproc in 1 ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" "$var" $nproc
    done
done

prog=lu_wf
var=NUM_THREADS
for nproc in `seq 1 16` ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" $var $nproc
    done
done

prog=lu_cilk++
var=CILK_NPROC
for nproc in `seq 1 16` ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" $var $nproc
    done
done
