#!/bin/bash

run()
{
    prog=$1
    flags=$2
    input=$3
    var=$4
    nproc=$5

    if [ $nproc -le 4 ] ; then
        echo -n $var=$nproc $prog CPU0-$[$nproc-1] mem0" "
        eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0 ./$prog > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
        if ! grep Running junk ; then echo ; fi
        rm junk
    else
        echo -n $var=$nproc $prog CPU0-$[$nproc-1] mem0" "
        eval "time $var=$nproc numactl --physcpubind 0-$[$nproc-1] --membind 0,1 ./$prog > junk" 2>&1 | tr '\n' ' ' | tr -s ' \t' '  '
        if ! grep Running junk ; then echo ; fi
        rm junk
    fi
    #echo
    #popd > /dev/null
}

input=
repeat=10
#repeat=1

prog=sparse_lu_wfo
var=NUM_THREADS
for nproc in `seq 1 16` ; do
    for run in `seq $repeat` ; do
	run $prog "$flags" "$input" $var $nproc
    done
done

#prog=sparse_lu_smpss
#var=CSS_NUM_CPUS
#for nproc in `seq 1 16` ; do
#    for run in `seq $repeat` ; do
#	run $prog "$flags" "$input" $var $nproc
#    done
#done
