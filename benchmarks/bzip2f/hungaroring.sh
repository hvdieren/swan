#!/bin/bash

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    echo -n $var=$nproc $prog $flags $input CPU0-$[$nproc-1] mem0-$[($nproc-1)/8]" "
    eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0-$[($nproc-1)/8] ./$prog -$flags -c $input > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
    if ! grep Running junk ; then echo ; fi
    rm junk
}

#input=bigfile
#input=bigfile10
input=input.compressed.250
#repeat=20
repeat=10

prog=bzip2_c
var=NONE
for flags in 9 ; do
    for nproc in 1 ; do
        for run in `seq $repeat` ; do
	    run $prog $flags $input "$var" $nproc
	done
    done
done

prog=bzip2_wf
var=NUM_THREADS
for flags in 9W 9w ; do
    for nproc in `seq 1 16` ; do
        for run in `seq $repeat` ; do
	    run $prog $flags $input $var $nproc
	done
    done
done

prog=bzip2_cilk++
var=CILK_NPROC
for flags in 9P 9p ; do
    for nproc in `seq 1 16` ; do
        for run in `seq $repeat` ; do
	    run $prog $flags $input $var $nproc
	done
    done
done

prog=/bin/bzip2
var=XXX
for flags in 9 ; do
    for nproc in 1 ; do
        for run in `seq $repeat` ; do
	    run $prog $flags $input $var $nproc
	done
    done
done
